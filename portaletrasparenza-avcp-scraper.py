#!/usr/bin/env python


'''Scraper for  http://portaletrasparenza.avcp.it website. Download 
   all the data available and publish it to a github repository.
   Usage (the tool should be used inside the data/ directory of the repository): 
      portaletrasparenza-avcp-scraper.py download (to download the data of closed tenders)
      portaletrasparenza-avcp-scraper.py download_open (to dowload the data of open tenders)
      portaletrasparenza-avcp-scraper.py indent (to indent in a clean way the data)
      portaletrasparenza-avcp-scraper.py push (to push the data on github)
   You can also combine the operation, so that they can be executed in series, for example:
      portaletrasparenza-avcp-scraper.py download indent push
   Downloads the data, indent it and it pushes it on a github 
'''



#Web interface documentation:
#First argument: search key (name or fiscal code) (-100 for wildcard)
#second: year
#third: month (-100 apprently for all the months for website generated queries)
#forth: 0 for closed tenders, 1 for acrive ones (-100 for wildcard)
#URL = portaletrasparenza.avcp.it/Microstrategy/asp/export_xml.aspx?valuepromptanswers=^^^
#warning: a query with all wildcars (-100^-100^-100^-100) will not result in dump of the database
#because the returned data would be hitting some sort of limit in result size

#TODO:
#openspending export
#fix bug in getting vendors with not-indented xml
#import aggregate winners
#import partecipants
#investigate other source of data from AVCP (CSV search save containts also the data of assignemnt of the contract? For a given CIG scrape more info URL?)
#reconciliate agency data with IndicePA
#reconciliate foreign vendors data with OpenCorporates
#reconciliate vendors data with AVCP Casellario delle Imprese
#handle appropriatly NoneAgencyName and NoneVendorName (and also ND and 99999999999 Agency codes)


import os
import sys
import requests
import getopt
import subprocess
import xml.dom.minidom
import codecs
import datetime
import xml.etree.ElementTree
import dataset
import sqlalchemy

#Year to scrape
years_to_download = range(2011,2016)
months_in_a_year = range(1,13)

database_name = "avcp_contracts.db"
database_url = "sqlite:///" + database_name

PUBLIC_AGENCIES = 'public_agencies'
VENDORS = 'vendors'
AGGREGATE_VENDORS = 'aggregate_vendors'
CONTRACTS = 'contracts'
WINNERS = 'winners'
AGGREGATE_WINNERS = 'aggregate_winners'
PUBLIC_AGENCIES_ALTERNATIVE_NAMES = 'public_agencies_alternative_names'
VENDORS_ALTERNATIVE_NAMES = 'vendors_alternative_names'
CIG_CODE_NAME = 'cig'
VENDOR_CODE = 'vendor_code'
AGENCY_CODE = 'agency_fiscal_code'
AGGREGATE_VENDOR_CODE = 'aggregate_vendor_code'

#Database buffer
database_buffer = {}
database_buffer_max_size = 10000
vendor_codes_to_commit = set()
vendor_names_to_commit = {}
agency_codes_to_commit = set()
agency_names_to_commit = {}
aggregate_vendor_codes_to_commit = set()
aggregate_vendor_names_to_commit = {}

#from http://stackoverflow.com/questions/16694907/how-to-download-large-file-in-python-with-requests-py
def download_file(url,filename=""):
    if( filename == "" ):
        local_filename = url.split('/')[-1]
    else:
        local_filename = filename
    # NOTE the stream=True parameter
    r = requests.get(url, stream=True)
    with open(local_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024): 
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
                f.flush()
    return local_filename

def iso_pubblication_date(pubblication_year,pubblication_month,separator="-"):
    return str(pubblication_year)+separator+'%02d' % pubblication_month;

def xml_filename(year,month,closed=True):
    if( closed ):
        return "avcp_contracts_"+iso_pubblication_date(year,month,"_")+".xml";
    else:
        return "avcp_contracts_"+iso_pubblication_date(year,month,"_")+"_open.xml";

def push_data_to_github():
    for year in years_to_download:
        for month in months_in_a_year:
            subprocess.call(["git","add",xml_filename(year,month)])
    
    commit_msg = "Data updated at " + datetime.datetime.now().isoformat()
    subprocess.call(["git","commit","-m",commit_msg])
    subprocess.call(["git","config","user.email",git_email])
    subprocess.call(["git","push"])
 
    return
  
def indent_data():
    for year in years_to_download:
        for month in months_in_a_year:
            filename = xml_filename(year,month)
            print("Indenting " + filename);
            xml_dom = xml.dom.minidom.parse(filename) 
            pretty_xml_as_string = xml_dom.toprettyxml(indent="  ")
            f = codecs.open(filename,'w','utf-8')
            f.write(pretty_xml_as_string)
            f.close()

def download_data():
    '''Download all the data available in AVCP http://portaletrasparenza.avcp.it for closed tenders
       in raw xml data. The data is splitted at each month to avoid reaching
       the maximum query limit'''
    print("Downloading data of closed tenders")

    for year in years_to_download:
        for month in months_in_a_year:
            url = "http://portaletrasparenza.avcp.it/Microstrategy/asp/export_xml.aspx?valuepromptanswers=-100^"+str(year)+"^"+str(month)+"^0"
            print("Downloading file " + xml_filename(year,month));
            download_file(url,xml_filename(year,month,True))
            
def download_data_active_tenders():
    print("Downloading data of active tenders")
    
    for year in years_to_download:
        for month in months_in_a_year:
            url = "http://portaletrasparenza.avcp.it/Microstrategy/asp/export_xml.aspx?valuepromptanswers=-100^"+str(year)+"^"+str(month)+"^1"
            print("Downloading file " + xml_filename(year,month));
            download_file(url,xml_filename(year,month,False))
            
def dataset_result_to_list_of_dict(dataset_result):
    return_value = []
    for result in dataset_result:
        return_value.append(result)
    return return_value

def buffered_insert(table,record,table_name):
    '''Insert a record in the table, but using an internal buffer to reduce the number of insert statements'''
    global agency_codes_to_commit
    global agency_names_to_commit
    global vendor_codes_to_commit
    global vendor_names_to_commit
    global aggregate_vendor_codes_to_commit
    global aggregate_vendor_names_to_commit
    
    #create buffer if it does not exist
    if( table_name not in database_buffer.keys() ):
        database_buffer[table_name] = []
        
    database_buffer[table_name].append(record)
    
    if( table_name == VENDORS ):
        vendor_codes_to_commit.add(record[VENDOR_CODE])
        vendor_names_to_commit[record[VENDOR_CODE]] = record['vendor_name']
        
    if( table_name == PUBLIC_AGENCIES ):
        agency_codes_to_commit.add(record[AGENCY_CODE])
        agency_names_to_commit[record[AGENCY_CODE]] = record['agency_name']
        
    if( table_name == AGGREGATE_VENDORS ):
        aggregate_vendor_codes_to_commit.add(record[AGGREGATE_VENDOR_CODE])
        aggregate_vendor_names_to_commit[record[AGGREGATE_VENDOR_CODE]] = record['aggregate_vendor_name']
    
    if( len(database_buffer[table_name]) > database_buffer_max_size ):
        print("Inserting rows for table " + table_name)
        table.insert_many(rows=database_buffer[table_name])
        database_buffer[table_name] = []
        
        if( table_name == VENDORS ):
            vendor_codes_to_commit = set()
            vendor_names_to_commit = {}
        
        if( table_name == PUBLIC_AGENCIES ):
            agency_codes_to_commit = set()
            agency_names_to_commit = {}
    
    

def flush_table_buffer(table,table_name):
    '''Flush buffer, for being sure that all uncommitted records are inserted in the database'''
    if( table_name in database_buffer.keys() ):
        print("Flushing " + str(len(database_buffer[table_name])) + " elements of table " + table_name);
        table.insert_many(rows=database_buffer[table_name])
        database_buffer[table_name] = []

def get_vendor_name(vendors_table,new_vendor_code):
    '''Get the vendor name given a vendor code, or None if the vendor_code is not present in the database'''
    if( new_vendor_code in vendor_codes_to_commit ):
        return vendor_names_to_commit[new_vendor_code]
    
    result = vendors_table.find_one(vendor_code=new_vendor_code)

    if( result is None ):
        return None
    else:
        return result['vendor_name']
    

def get_aggregate_vendor_name(aggregate_vendors_table,new_aggregate_vendor_code):
    '''Get the aggregate vendor name given a vendor code, or None if the aggregate_vendor_code is not present in the database'''
    if( new_aggregate_vendor_code in aggregate_vendor_codes_to_commit ):
        return aggregate_vendor_names_to_commit[new_aggregate_vendor_code]
    
    result = aggregate_vendors_table.find_one(aggregate_vendor_code=new_aggregate_vendor_code)

    if( result is None ):
        return None
    else:
        return result['aggregate_vendor_name']
    
def get_agency_name(agencies_table,new_agency_code):
    '''Get the agency name given a agency code, or None if the agency_code is not present in the database'''
    if( new_agency_code in agency_codes_to_commit ):
        return agency_names_to_commit[new_agency_code]
    
    result = agencies_table.find_one(agency_fiscal_code=new_agency_code)

    if( result is None ):
        return None
    else:
        return result['agency_name']
    
def add_vendor(db,new_vendor_code,new_vendor_name,foreign_vendor=False):
    '''Add a vendor, if the vendor_code is already in the database and the vendor name does not match,
       add the vendor name to VENDORS_ALTERNATIVE_NAMES table'''
    vendors_table = db.get_table(VENDORS)
    vendors_alternative_names_table = db.get_table(VENDORS_ALTERNATIVE_NAMES)
    
    if( new_vendor_name is None ):
        new_vendor_name = "NoneVendorName"+new_vendor_code;
    
    #if( len(new_vendor_code) != 11 and len(new_vendor_code) != 16 and not foreign_vendor ):
        #print("Warning: vendor with fiscal code " + new_vendor_code + " ( " + (new_vendor_name) + (" ) not conformant to the fiscal code specification"))

    #print("Called add_vendor with vendor_name " + new_vendor_name)

    found_vendor_name = get_vendor_name(vendors_table,new_vendor_code);
    
    if( found_vendor_name is None ):
        #adding a new vendor to the table
        vendor_row = {}
        vendor_row[VENDOR_CODE] = new_vendor_code
        vendor_row['vendor_name'] = new_vendor_name
        if( not foreign_vendor ):
            vendor_row['vendor_country'] = "ITALY"
        else:
            vendor_row['vendor_country'] = "NOT ITALY"
        #vendors_table.insert(vendor_row)
        buffered_insert(vendors_table,vendor_row,VENDORS);
        
    else:
        #vendor already present, if with another name adding the alternative name to vendors_alternative_names_table
        if( not( found_vendor_name == new_vendor_name ) ):
            alternative_name_row = {}
            alternative_name_row[VENDOR_CODE] = new_vendor_code
            alternative_name_row['alternative_vendor_name'] = new_vendor_name
            #vendors_alternative_names_table.insert(alternative_name_row);
            buffered_insert(vendors_alternative_names_table,alternative_name_row,VENDORS_ALTERNATIVE_NAMES)
            
def add_agency(db,new_agency_code,new_agency_name):
    '''Add an agency, if the new_agency_code is already in the database and the agency name does not match,
       add the agency name to PUBLIC_AGENCIES_ALTERNATIVE_NAMES table'''
    agencies_table = db.get_table(PUBLIC_AGENCIES)
    agencies_alternative_names_table = db.get_table(PUBLIC_AGENCIES_ALTERNATIVE_NAMES)
    
    if( new_agency_name is None ):
        new_agency_name = "NoneAgencyName"+new_agency_code;

    #if( len(new_agency_code) != 11 ):
        #print("Warning: agency with fiscal code " + new_agency_code + " ( " + new_agency_name + " ) not conformant to the fiscal code specification")

    #print("Called add_agency with new_agency_name " + new_agency_name + " and new_agency_code " + new_agency_code)

    found_agency_name = get_agency_name(agencies_table,new_agency_code);

    if( found_agency_name is None ):
        #adding a new vendor to the table
        agency_row = {}
        agency_row[AGENCY_CODE] = unicode(new_agency_code)
        agency_row['agency_name'] = unicode(new_agency_name)
        #agencies_table.insert(agency_row)
        buffered_insert(agencies_table,agency_row,PUBLIC_AGENCIES);
    else:
        #vendor already present, if with another name adding the alternative name to vendors_alternative_names_table
        if( not( found_agency_name == new_agency_name ) ):
            alternative_name_row = {}
            alternative_name_row[AGENCY_CODE] = new_agency_code
            alternative_name_row['alternative_agency_name'] = new_agency_name
            #agencies_alternative_names_table.insert(alternative_name_row);
            buffered_insert(agencies_alternative_names_table,alternative_name_row,PUBLIC_AGENCIES_ALTERNATIVE_NAMES)

def add_aggregate_vendor(db,aggregate_vendors_dict):
    vendors_table = db.get_table(VENDORS)
    aggregate_vendors_table = db.get_table(AGGREGATE_VENDORS)
    aggregate_winners_table = db.get_table(AGGREGATE_WINNERS)
    aggregate_vendors_dict.sort(lambda x,y : cmp(x['code'], y['code']))
    aggregate_vendor_code = ""
    aggregate_vendor_name = ""
    first_item = True
    for item in aggregate_vendors_dict:
        if( first_item ):
            aggregate_vendor_code = item["code"]
        else:
            aggregate_vendor_code = aggregate_vendor_code+"-"+item["code"]
        found_vendor_name = get_vendor_name(vendors_table,item["code"]);
        if( found_vendor_name is None ):
            assert(False)
        print("Vendor name: " + found_vendor_name)
        
        if( first_item ):
            aggregate_vendor_name = found_vendor_name
        else:
            aggregate_vendor_name = aggregate_vendor_name + " | " + found_vendor_name  
        first_item = False

    
    print("Called add_aggregate_vendor: " + aggregate_vendor_name + " ( " + aggregate_vendor_code + " ) ")
    
    found_aggregate_vendor_name = get_aggregate_vendor_name(aggregate_vendors_table,aggregate_vendor_code);

    if( found_aggregate_vendor_name is None ):
        aggregate_vendor_row = {}
        aggregate_vendor_row[AGGREGATE_VENDOR_CODE] = aggregate_vendor_code
        aggregate_vendor_row['aggregate_vendor_name'] = aggregate_vendor_name
        buffered_insert(aggregate_vendors_table,aggregate_vendor_row,AGGREGATE_VENDORS)
        
    return aggregate_vendor_code

def extract_data_from_file(db,filename,pubblication_year,pubblication_month):
    '''Extract data from a file to the given database'''
    
    #load tables from database 
    public_agencies_table = db.get_table(PUBLIC_AGENCIES)
    vendors_table = db.get_table(VENDORS)
    aggregate_vendors_table = db.get_table(AGGREGATE_VENDORS)
    contracts_table = db.get_table(CONTRACTS)
    winners_table = db.get_table(WINNERS)
    aggregate_winners_table = db.get_table(AGGREGATE_WINNERS)
    public_agencies_alternative_names_table = db.get_table(PUBLIC_AGENCIES_ALTERNATIVE_NAMES)
    vendors_alternative_names_table = db.get_table(VENDORS_ALTERNATIVE_NAMES)
    
    print("extract_data_from_file of year " + str(pubblication_year) + " month " + str(pubblication_month))
    
    tree = xml.etree.ElementTree.parse(filename)
    root = tree.getroot()
    for lotto in root.iter('lotto'):
        #inserting the contract
        contract_row = {}
        if( len(lotto.find('cig').text) > 10 ):
            print("Warning: CIG " + lotto.find('cig').text + " not conformant to the AVCP specification")
        contract_row[CIG_CODE_NAME] = lotto.find('cig').text
        contract_row['oggetto'] = lotto.find('oggetto').text 
        contract_row['sceltaContraente'] = lotto.find('sceltaContraente').text
        contract_row['importoAggiudicazione'] = lotto.find('importoAggiudicazione').text
        contract_row['importoSommeLiquidate'] = lotto.find('importoSommeLiquidate').text
        contract_row[AGENCY_CODE] = lotto.find('strutturaProponente').find('codiceFiscaleProp').text
        contract_row['pubblicationMonth'] = pubblication_month
        contract_row['pubblicationYear'] = pubblication_year
        contract_row['pubblication_date'] = iso_pubblication_date(pubblication_year,pubblication_month)
        #contracts_table.insert(contract_row)
        buffered_insert(contracts_table,contract_row,CONTRACTS);
        
        #inserting the public agency (if present)
        agency_code = lotto.find('strutturaProponente').find('codiceFiscaleProp').text
        agency_name = lotto.find('strutturaProponente').find('denominazione').text
        add_agency(db,agency_code,agency_name)
        
        #inserting the winner (if present)
        if( lotto.find('aggiudicatari') is not None ):
            if( lotto.find('aggiudicatari').find('aggiudicatario') is not None and lotto.find('aggiudicatari').find('aggiudicatario').find('ragioneSociale') is not None ):
                if( lotto.find('aggiudicatari').find('aggiudicatario').find('codiceFiscale') is None ):
                    #Foreing vendor 
                    vendor_code = lotto.find('aggiudicatari').find('aggiudicatario').find('identificativoFiscaleEstero').text
                    vendor_foreigner = True
                else:
                    vendor_code = lotto.find('aggiudicatari').find('aggiudicatario').find('codiceFiscale').text
                    vendor_foreigner = False
                vendor_name = lotto.find('aggiudicatari').find('aggiudicatario').find('ragioneSociale').text
                add_vendor(db,vendor_code,vendor_name,vendor_foreigner)
                #adding winner
                winner_row = {}
                winner_row[CIG_CODE_NAME] = contract_row[CIG_CODE_NAME]
                winner_row[VENDOR_CODE] = vendor_code
                buffered_insert(winners_table,winner_row,WINNERS)
                
            if( lotto.find('aggiudicatari').find('aggiudicatarioRaggruppamento') is not None):
                if( lotto.find('aggiudicatari').find('aggiudicatarioRaggruppamento').find('membro') is not None ):
                    raggruppamento = lotto.find('aggiudicatari').find('aggiudicatarioRaggruppamento');
                    aggregate_vendors_dict = []
                    for membro in raggruppamento.iter('membro'):
                        if( membro.find('codiceFiscale') is None):
                            #foreign vendor 
                            vendor_code = membro.find('identificativoFiscaleEstero').text
                            vendor_foreigner = True
                        else:
                            vendor_code = membro.find('codiceFiscale').text
                            vendor_foreigner = False
                        vendor_name = membro.find('ragioneSociale').text;
                        vendor_role = membro.find('ruolo').text;
                        add_vendor(db,vendor_code,vendor_name,vendor_foreigner);
                        aggregate_vendors_dict.append(dict(code=vendor_code,role=vendor_role));
                    aggregate_vendor_code = add_aggregate_vendor(db,aggregate_vendors_dict);
                    winner_row = {}
                    winner_row[CIG_CODE_NAME] = contract_row[CIG_CODE_NAME]
                    winner_row[AGGREGATE_VENDOR_CODE] = aggregate_vendor_code
                    buffered_insert(aggregate_winners_table,winner_row,AGGREGATE_WINNERS)
                 
         
    
def extract_data():
    '''Extract data from the downloaded xml files'''
    subprocess.call(['rm',database_name])
    
    
    db = dataset.connect(database_url)
    
    #this data should be loaded from indicepa
    db.create_table(PUBLIC_AGENCIES, primary_id=AGENCY_CODE, primary_type='Text')
    db.create_table(VENDORS, primary_id=VENDOR_CODE,primary_type='Text')
    db.create_table(AGGREGATE_VENDORS,primary_id='aggregate_vendor_code',primary_type='Text')
    db.create_table(CONTRACTS, primary_id=CIG_CODE_NAME, primary_type='Text')
    db.create_table(WINNERS, primary_id=CIG_CODE_NAME, primary_type='Text')
    db.create_table(AGGREGATE_WINNERS, primary_id=CIG_CODE_NAME, primary_type='Text')
    db.create_table(PUBLIC_AGENCIES_ALTERNATIVE_NAMES)
    db.create_table(VENDORS_ALTERNATIVE_NAMES)
    
    
    for year in years_to_download:
        for month in months_in_a_year:
            filename = xml_filename(year,month)
            extract_data_from_file(db,filename,year,month)
            
    flush_table_buffer(db.get_table(PUBLIC_AGENCIES),PUBLIC_AGENCIES);
    flush_table_buffer(db.get_table(VENDORS),VENDORS);
    flush_table_buffer(db.get_table(AGGREGATE_VENDORS),AGGREGATE_VENDORS);
    flush_table_buffer(db.get_table(CONTRACTS),CONTRACTS);
    flush_table_buffer(db.get_table(WINNERS),WINNERS);
    flush_table_buffer(db.get_table(AGGREGATE_WINNERS),AGGREGATE_WINNERS);
    flush_table_buffer(db.get_table(PUBLIC_AGENCIES_ALTERNATIVE_NAMES),PUBLIC_AGENCIES_ALTERNATIVE_NAMES);
    flush_table_buffer(db.get_table(VENDORS_ALTERNATIVE_NAMES),VENDORS_ALTERNATIVE_NAMES);


def dump_all_winners():
    
    db = dataset.connect(database_url)
    
    db.query('CREATE TABLE all_winners AS SELECT winners.cig AS cig, vendors.vendor_code AS vendor_code, vendors.vendor_name AS vendor_name FROM winners JOIN vendors ON winners.vendor_code = vendors.vendor_code UNION SELECT aggregate_winners.cig AS cig, aggregate_vendors.aggregate_vendor_code AS vendor_code, aggregate_vendors.aggregate_vendor_name AS vendor_name FROM aggregate_winners JOIN aggregate_vendors ON aggregate_winners.aggregate_vendor_code = aggregate_vendors.aggregate_vendor_code');
    
def process(arg):
    if( arg == "download" ):
        download_data()
    if( arg == "indent" ):
        indent_data()
    if( arg == "push" ):
        push_data_to_github()
    if( arg == "extract" ):
        extract_data()
    if( arg == "dump_all_winners" ):
        dump_all_winners()
   
def main():
    '''Main method for the scraper'''
        # parse command line options
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h", ["help"])
    except getopt.error:
        print("for help use --help")
        sys.exit(2)
        
    # process options
    for o, a in opts:
        if o in ("-h", "--help"):
            print(__doc__)
            sys.exit(0)
        if o == "--github_user":
            print("Using github user " + a)
        if o == "--github_password":
            print("Using github password " + a)
            
    # process arguments
    for arg in args:
        process(arg) # process() is defined elsewhere
        
    

if __name__ == "__main__":
    main()
