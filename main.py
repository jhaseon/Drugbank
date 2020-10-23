"""
file: main.py
date: 10/23/20
input: drugbank ids #NOTE: ids are hardcoded into script. they can be extracted from the site or givin in a separate file.
function: to collect data from drugbank.com 
#NOTE: this can be compartmentalized based on input to collect different data points by creating modules for extracting, tranforming, loading data.  
            1. Drugbank ID (Accession Number)
            2. SMILES
            3. Actions (Pharmacological actions equivalent)
            4. Gene Name 
            5. Alternative IDs 
output: postgre db.
"""

from bs4 import BeautifulSoup #NOTE: A faster parser/package can be used to increase speed. 
import requests
import datetime
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def sendtodb(sql, final_dict):
    try:
        conn = get_connection("drugbank")
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()

        cursor.execute(sql, final_dict)

        print("drugbank inserted") #NOTE: include to log what database and table is inserted

        cursor.close()
        conn.close()

    except psycopg2.Error as e:
        print(type(e)) #NOTE: include to log what database and table is inserted and what error
        print(e) #NOTE: include to log what database and table is inserted and what error

def get_connection(dbname = "postgres"):
    user = "postgres"
    password = "n%:Wx{n%<ygk;7d^"
    connect_str = "dbname={} host='localhost' user='{}' password='{}'".format(dbname, user, password)

    return psycopg2.connect(connect_str)

def decode(cfemail):
    enc = bytes.fromhex(cfemail)
    return bytes([c ^ enc[0] for c in enc[1:]]).decode('utf8')

def deobfuscate_cf_email(soup): 
    """
    The smiles string contains "@" which is obfusated by the site to prevent collection of emails.
    """
    for encrypted_email in soup.select('a.__cf_email__'):
        decrypted = decode(encrypted_email['data-cfemail'])
        script_tag = encrypted_email.find_next_sibling('script')
        encrypted_email.replace_with(decrypted)

## List of drugbank IDs
# NOTE: this can be extracted from a different external source
drugbank_id_list = ["DB00619", "DB01048", "DB14093", "DB00173", "DB00734", "DB00218", "DB05196",
"DB09095", "DB01053", "DB00274"]

# drugbank_id_list = ["DB01066"]

for id in drugbank_id_list:
    print(id) #NOTE: Log what id is being ran

    ## Ingest webpage using Beautifulsoup to parse html
    page = requests.get("https://go.drugbank.com/drugs/{}".format(id))
    soup = BeautifulSoup(page.content, 'html.parser')
    deobfuscate_cf_email(soup)
    card_content = soup.find_all("div", "card-content px-md-4 px-sm-2 pb-md-4 pb-sm-2")[0]
    actions_list = []
    gene_name_list = []
    final_dict = {}

    for index, header in enumerate(card_content.contents):
        ## Accession Number 
        if header.contents == ["Identification"]:
            body = card_content.contents[index + 1]
            for index, label in enumerate(body.contents):
                if label.contents == ["Accession Number"]:
                    value = body.contents[index + 1]
                    accession_number = value.contents[0]
                    final_dict['drugbankid'] = accession_number
                    
        ## SMILES: issue with the @ sign (done)
        if header.contents == ["Chemical Identifiers"]:
            body = card_content.contents[index + 1]
            for index, label in enumerate(body.contents):
                if label.contents == ["SMILES"]:
                    value = body.contents[index + 1]
                    smiles = ''.join(value.contents[0].contents)
                    final_dict['smiles'] = smiles

        ## Gene name: Could have multiple targets thus multiple gene names (done)
        if header.get('id') == 'targets':
            bond_list = header.find("div", class_='bond-list')
            for bond_card in bond_list:
                for bond_card_content in bond_card:
                    if bond_card_content.get('class') == ['card-body']:
                        if not bond_card_content: ## if bond_card doesn't exist
                            bond_card_rows = bond_card_content.find("div", class_ = "col-sm-12 col-lg-7").contents[0]
                            for index, bond_card_row in enumerate(bond_card_rows):
                                if bond_card_row.contents == ["Gene Name"]:
                                    gene_name = bond_card_rows.contents[index + 1].contents[0]
                                    gene_name_list.append(gene_name)
                        else:
                            gene_name_list.append("")

        ## Actions: Pharmacological action not needed 
        if header.get('id') == 'targets':
            bond_list = header.find("div", class_='bond-list')
            for bond_card in bond_list:
                for bond_card_content in bond_card:
                    if bond_card_content.get('class') == ['card-body']:  
                        if not bond_card_content: ## if bond_card doesn't exist
                            bond_card_rows = bond_card_content.find("div", class_ = "col-sm-12 col-lg-5").contents[0]
                            for index, bond_card_row in enumerate(bond_card_rows):
                                if bond_card_row.contents == ["Actions"]:
                                    actions = bond_card_rows.contents[index + 1].contents[0].contents[0]
                                    actions_list.append(actions)
                        else:
                            actions_list.append("")

        ## Alternative Identifiers: Include database location. Ex: "KEGG Drug~D03136"
        ## Issues: Drugs.com, PDRhealth, RxList #NOTE: the values for these return not useful information. Will need to parse into reference link to gather more useful information.
        if header.contents == ["References"]:
            body = card_content.contents[index + 1]
            for index, label in enumerate(body.contents):
                if label.contents == ["External Links"]:
                    value = body.contents[index + 1]
                    alt_id_dict = {}
                    for index in range(len(value.contents[0].contents)):
                        if index % 2 == 0:
                            alt_id_dict[value.contents[0].contents[index].contents[0]] = value.contents[0].contents[index + 1].contents[0].contents[0]
                        if index % 2 != 0:
                            continue

    ## Send to database #NOTE: This steps below can be separated into a different script (eg. load.py) to hold logic necessary for specific datapoints 
    sql_string = """INSERT INTO drugs(drugbankid, smiles) 
                VALUES (%(drugbankid)s, %(smiles)s);"""
    sendtodb(sql_string, final_dict)
    
    ## Create final_alternative_id_dict
    final_alternative_id_dict = {}
    final_alternative_id_dict['drugbankid'] = final_dict['drugbankid']
    for item in alt_id_dict:
        final_alt_id = "{}~{}".format(item, alt_id_dict[item])
        final_alternative_id_dict['alt_ids'] = final_alt_id
        sql_string = """INSERT INTO alternative_ids(drugbankid, alt_ids) 
                VALUES (%(drugbankid)s, %(alt_ids)s);"""
        sendtodb(sql_string, final_alternative_id_dict)

    ## Create final_targets_dict
    final_targets_dict = {}
    final_targets_dict['drugbankid'] = final_dict['drugbankid']
    for index, item in enumerate(actions_list): 
        final_targets_dict['actions'] = item
        final_targets_dict['gene_name'] = gene_name_list[index]
        sql_string = """INSERT INTO targets(drugbankid, actions, gene_name) 
                    VALUES (%(drugbankid)s, %(actions)s, %(gene_name)s);"""
        sendtodb(sql_string, final_targets_dict)

    
    