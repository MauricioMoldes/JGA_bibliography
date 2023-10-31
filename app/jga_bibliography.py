#!/usr/bin/env python

"""jga_bibliography.py: Gets the study ID's from JGA, discovers new JGA publications """
import traceback

__author__ = "Mauricio Moldes"
__version__ = "0.1"
__maintainer__ = "Mauricio Moldes"
__email__ = "mauricio.moldes@crg.eu"
__status__ = "development"

from json import JSONDecodeError
from config import config, legacy, blocked_accessions
import sys
import requests
import psycopg2
from xml.dom import minidom
import datetime
import time
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import yaml

logger = logging.getLogger('jga_bibliography_logger')

''' VERIFIES THE CONNECTION TO PLSQL '''


def connection_plsql():
    conn_string = "host='" + str(cfg['plsql_staging']['host']) + "' dbname='" + str(
        cfg['plsql_staging']['dbname']) + "' user='" + str(
        cfg['plsql_staging']['user']) + "' password='" + str(cfg['plsql_staging']['password']) + "' port = '" + str(
        cfg['plsql_staging']['port']) + "'"
    conn_plsql = psycopg2.connect(conn_string)
    return conn_plsql


""" GETS ALL THE JGA STUDY ID'S """


def get_src_jga_studies():
    try:
        response = requests.get("https://ddbj.nig.ac.jp/resource/jga-dac/JGAC000001.json", timeout=30)
        response.raise_for_status()
        # Code here will only run if the request is successful
        response_info = response.json()
        return response_info
    except requests.exceptions.HTTPError as errh:
        return errh
    except requests.exceptions.ConnectionError as errc:
        return errc
    except requests.exceptions.Timeout as errt:
        return errt
    except requests.exceptions.RequestException as err:
        return err


""" GETS ALL THE jga DATASET ID'S """


def get_src_jga_datasets(conn_plsql):
    cursor = conn_plsql.cursor()
    cursor.execute(" select jga_stable_id from dataset_table where disabled_flag=false ")
    records = cursor.fetchall()
    return records


""" INSERT STUDY IN BIBLIOGAPHY.jga_study """


def insert_study(data, conn_plsql):
    for study, europubmed in data.items():
        cur = conn_plsql.cursor()
        sql = "INSERT INTO \"bibliography\".jga_study (jga_accession_id)" + " SELECT %s " \
              + " WHERE NOT EXISTS (SELECT jga_accession_id FROM \"bibliography\".jga_study WHERE jga_accession_id =%s);"
        cur.execute(sql, (study, study))
        conn_plsql.commit()
        cur.close()


""" THROTTLE QUERY"""


def throttle_query(url):
    session = requests.Session()  # creates a session object that allows the persistence of certain paramenters across requests.
    retry = Retry(connect=2,
                  backoff_factor=0.5)  # maximum connections after connection related erros, and backoff factor apply between attempts
    adapter = HTTPAdapter(max_retries=retry)  # http adapter receives retry parameters
    session.mount('https://', adapter)  # mounts session
    response_core = session.get(
        url)  # Request object which will be sent off to a server to request or query some resource. Second, a Response object is generated once requests gets a response back from the server.
    session.close()  # closes session
    return response_core  # returns reponse


""" PARSE EUROPUBMED CITATIONS """


def parse_europubmed_citations(data, conn):
    citations = []
    for key, value in data.items():
        logger.info("Parsing publication  " + str(key))
        results = (len(value['citationList']["citation"]))
        i = 0
        while i < results:
            try:
                pmid = (value["citationList"]["citation"][i]["id"])
                citations.append(pmid)
                i = i + 1
            except:
                pmid = ""
    return citations


""" AUXILIAR METHOD, PARSES THE VALUE FROM THE JSON REPONS """


def parse_json_value(i, value, argument):
    try:
        res = (value["resultList"]["result"][i][argument])
    except:
        # declare empty attribute
        res = None
        logger.debug("Attribute: %s is empty" % (argument,))
    return res


""" CONVERTS Y,N  TO T,F FOR PLSQL INSERTION PURPOSES """


def convert_value_to_plsql(value):
    if value == "Y":
        res = "t"
    else:
        res = "f"
    return res


""" PARSE EUROPUBMED RESPONSE"""


def parse_europubmed_basic(data, conn_plsql, action):
    blocked_pubmed = blocked_accessions.blocked_accessions.get("pubmed")
    for key, value in data.items():
        logger.info("Parsing study " + str(key))
        results = (len(value['resultList']["result"]))
        i = 0
        while i < results:
            pmid = parse_json_value(i, value, "id")
            # verifies pre-publication, skips those (PPR), also pubmed central(PMC) entries.
            if "PPR" in pmid or "PMC" in pmid or "IND" in pmid:
                i = i + 1
                continue
            # verifies if pubmeid is in blocked list, skips.
            elif pmid in blocked_pubmed:
                i = i + 1
                continue
            # verifies if study is in block list, skips.
            elif key in blocked_accessions.blocked_accessions:
                # verifies if any pubmed_accession is blocked for this jga_accession
                blocked_pmid = blocked_accessions.blocked_accessions.get(key)
                if pmid in blocked_pmid:
                    i = i + 1
                    continue
            else:
                source = parse_json_value(i, value, "source")
                doi = parse_json_value(i, value, "doi")
                title = parse_json_value(i, value, "title")
                author_string = parse_json_value(i, value, "authorString")
                journal_title = parse_json_value(i, value, "journalTitle")
                issue = parse_json_value(i, value, "issue")
                journal_volume = parse_json_value(i, value, "journalVolume")
                publication_year = parse_json_value(i, value, "pubYear")
                first_publication_date = parse_json_value(i, value, "firstPublicationDate")
                journal_issn = parse_json_value(i, value, "journalIssn")
                page_information = parse_json_value(i, value, "pageInfo")
                publication_type = parse_json_value(i, value, "pubType")
                is_open_access_aux = parse_json_value(i, value, "isOpenAccess")
                is_open_access = convert_value_to_plsql(is_open_access_aux)
                in_epmc_aux = parse_json_value(i, value, "inEPMC")
                in_epmc = convert_value_to_plsql(in_epmc_aux)
                in_pmc_aux = parse_json_value(i, value, "inPMC")
                in_pmc = convert_value_to_plsql(in_pmc_aux)
                cited_by_count = parse_json_value(i, value, "citedByCount")
                has_references_aux = parse_json_value(i, value, "hasReferences")
                has_references = convert_value_to_plsql(has_references_aux)
                has_text_mined_terms_aux = parse_json_value(i, value, "hasTextMinedTerms")
                has_text_mined_terms = convert_value_to_plsql(has_text_mined_terms_aux)
                has_db_cross_references_aux = parse_json_value(i, value, "hasDbCrossReferences")
                has_db_cross_references = convert_value_to_plsql(has_db_cross_references_aux)
                has_labs_links_aux = parse_json_value(i, value, "hasLabsLinks")
                has_labs_links = convert_value_to_plsql(has_labs_links_aux)
                has_tma_accession_numbers_aux = parse_json_value(i, value, "hasTMAccessionNumbers")
                has_tma_accession_numbers = convert_value_to_plsql(has_tma_accession_numbers_aux)
                ts = time.time()
                created = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
                updated = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

                insert_article(conn_plsql, pmid, source, doi, title, author_string, journal_title, issue, \
                               journal_volume, publication_year, first_publication_date, journal_issn, page_information,
                               publication_type,
                               cited_by_count, is_open_access, in_epmc, in_pmc, has_references, has_text_mined_terms,
                               has_db_cross_references,
                               has_labs_links, has_tma_accession_numbers, created, updated)

                if action == "CITATION":
                    insert_src_tgt_pubmed(conn_plsql, key, pmid)
                else:
                    insert_jga_study_article(conn_plsql, key, pmid)
                i = i + 1
        # end study parser


""" INSERTS THE RELATIONSHIP BETWEEN jga STUDY TO ARTICLE  """


def insert_jga_study_article(connLocalBibliography, jga_study, article):
    cursor = connLocalBibliography.cursor()
    sql = "INSERT INTO  \"bibliography\".jga_study_article (jga_accession_id, article_id) " \
          " SELECT %s, %s WHERE NOT EXISTS (  SELECT jga_accession_id , article_id" + \
          " FROM  \"bibliography\".jga_study_article" + " WHERE jga_accession_id = %s AND article_id = %s );"
    cursor.execute(sql, (jga_study, article, jga_study, article))
    connLocalBibliography.commit()
    cursor.close()


""" INSERTS ARTICLE """


def insert_article(conn_plsql, pmid, source, doi, title, author_string, journal_title, issue, \
                   journal_volume, publication_year, first_publication_date, journal_issn, page_information,
                   publication_type, cited_by, \
                   is_open_access, in_epmc, in_pmc, has_references, has_text_mined_terms, has_db_cross_references,
                   has_lab_links, has_tma_accession_numbers, created, updated):
    cursor = conn_plsql.cursor()

    sql = "INSERT INTO  \"bibliography\".article (article_id, source,doi,title,author_string,journal_title,issue," \
          "journal_volume,publication_year,first_publication_date ,journal_issn,page_information,publication_type,cited_by,is_open_access,  " \
          "in_epmc,in_pmc,has_references,has_text_mined_terms,has_db_cross_references,has_labs_links," \
          "has_tma_accession_numbers,created,last_updated)" \
          " SELECT %s,%s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s" \
          " WHERE NOT EXISTS (SELECT article_id " \
          " FROM \"bibliography\".article" \
          " WHERE article_id =%s);"

    sql_update = "UPDATE \"bibliography\".article" \
                 " SET " \
                 " article_id = %s, source= %s,  doi= %s,  author_string= %s, journal_title= %s,  issue= %s, " \
                 " journal_volume= %s, publication_year= %s, first_publication_date= %s,journal_issn= %s, page_information= %s," \
                 " publication_type= %s, cited_by= %s, is_open_access= %s,  in_epmc= %s, in_pmc= %s," \
                 " has_references= %s, has_text_mined_terms= %s, has_db_cross_references= %s, has_labs_links= %s," \
                 " has_tma_accession_numbers= %s, created= %s , last_updated= %s  WHERE article_id = %s ;"

    cursor.execute(sql, (pmid, source, doi, title, author_string, journal_title, issue, journal_volume,
                         publication_year, first_publication_date, journal_issn, page_information, publication_type,
                         cited_by, is_open_access, in_epmc, in_pmc, has_references, has_text_mined_terms,
                         has_db_cross_references, has_lab_links, has_tma_accession_numbers, created, updated, pmid))

    conn_plsql.commit()

    cursor.execute(sql_update, (pmid, source, doi, author_string, journal_title, issue, journal_volume,
                                publication_year, first_publication_date, journal_issn, page_information,
                                publication_type,
                                cited_by, is_open_access, in_epmc, in_pmc, has_references, has_text_mined_terms,
                                has_db_cross_references, has_lab_links, has_tma_accession_numbers, created, updated,
                                pmid))
    conn_plsql.commit()
    cursor.close()


""" QUERIES ERUOPUBMED FOR CITATIONS """


def query_europubmed_citations(src_article_id):
    found_europubmed = {}
    url = (
            str(config.europubmed_base_url) + "MED/" + src_article_id + "/citations?page=1&pageSize=1000&format=json")
    response_core = throttle_query(url)
    if response_core.status_code == requests.codes.ok:
        try:
            data = response_core.json()
            hitcount = (data["hitCount"])
            if hitcount == 0:
                pass
            else:
                found_europubmed.update({src_article_id: data})
        except (ValueError, JSONDecodeError) as e:
            logger.warning("Error getting citations for %s . The exceptions is : {} ".format(e), src_article_id)
            return {}
    return found_europubmed


""" QUERY EUROPUBMED FOR A EUROPUBMED ID"""


def query_europubmed_id(study, europubmeid):
    found_europubmed = {}
    url = (str(config.europubmed_base_url) + "search?query=ext_id:" + str(
        europubmeid) + "%20src:med&format=json&pageSize=1000")
    response_core = throttle_query(url)
    if response_core.status_code == requests.codes.ok:
        data = response_core.json()
        if data and "hitCount" in data:
            hitcount = (data["hitCount"])
            if hitcount == 0:
                pass
            else:
                found_europubmed.update({study: data})
    return found_europubmed


""" QUERY EUROPUBMED FOR AN ACCESSION """


def query_europubmed_simple(study):
    found_studies = {}
    url = str(config.europubmed_base_url) + "search?query=" + str(study) + "&pageSize=1000&format=json"
    response_core = throttle_query(url)
    data = response_core.json()
    hitcount = (data["hitCount"])
    if hitcount == 0:
        pass
    else:
        found_studies.update({study: data})
    return found_studies


""" QUERY EUROPUBMED FOR AN ACCESSION """


def query_europubmed_simple_dataset(study, dataset):
    found_studies = {}
    url = (
        (str(config.europubmed_base_url) + "search?query=" + str(dataset) + "&pageSize=1000&format=json"))
    response_core = throttle_query(url)
    data = response_core.json()
    hitcount = (data["hitCount"])
    if hitcount == 0:
        pass
    else:
        print(str(study[0]) + " = " + str(dataset) + " : " + str(hitcount))
        found_studies.update({study: data})
    return found_studies


""" CONVERTS THE DATASET ACCESSION TO STUDY"""


def convert_dataset_study(conn_plsql, dataset):
    cursor = conn_plsql.cursor()
    sql = "SELECT study_table.jga_stable_id " \
          " FROM study_dataset_table" \
          " INNER JOIN dataset_table ON study_dataset_table.dataset_id = dataset_table.\"id\" " \
          " INNER JOIN study_table ON study_dataset_table.study_id = study_table.\"id\" " \
          " where dataset_table.jga_stable_id = %s and study_dataset_table.disabled_flag=false"
    cursor.execute(sql, dataset)
    records = cursor.fetchone()
    return records


""" GETS THE jga STUDY AND THEIR XML """


def query_xml_studies(conn_plsql):
    cursor = conn_plsql.cursor()
    cursor.execute("select jga_stable_id, ebi_xml from study_table where repository ='jga' and disabled_flag=false ")
    records = cursor.fetchall()
    return records


""" XML MAIN METHOD  """


def xml_studies(conn_plsql):
    xml_studies = query_xml_studies(conn_plsql)
    for study in xml_studies:
        study_id = study[0]
        study_xml = study[1]
        if not study_xml:  # many ljgacy studies  have no XML
            pass
        else:
            publications = parse_xml_study(study_id, study_xml)
            if publications:
                for europubmed_id in publications:
                    if europubmed_id.startswith("0000"):
                        logger.info("Invalid europubmed : " + str(europubmed_id))
                    else:
                        data = query_europubmed_id(study_id, europubmed_id)
                        insert_study(data, conn_plsql)
                        parse_europubmed_basic(data, conn_plsql, "XML")


""" PARSES STUDY XML """


def parse_xml_study(study, study_xml):
    study_publications = []
    try:
        dom = minidom.parseString(study_xml)
        publications = dom.getElementsByTagName('XREF_LINK')
        if publications:
            for publication in publications:
                pub_id_node = publication.getElementsByTagName("ID")[0]
                europubmed_id = pub_id_node.firstChild.data
                study_publications.append(europubmed_id)
        else:
            pass
    except Exception as e:
        print(e)
    return study_publications


""" LEGACY STUDIES - HARDCODED LEGACY_EUROPUBMED RELATION """


def legacy_studies(conn_plsql):
    legacy_study_pubmed = legacy.legacy_study_pubmed

    for study, pubmed in legacy_study_pubmed.items():
        data = query_europubmed_id(study, pubmed)
        insert_study(data, conn_plsql)
        parse_europubmed_basic(data, conn_plsql, "LEGACY")


def get_study(study):
    try:
        response = requests.get("https://ddbj.nig.ac.jp/resource/jga-study/" + study + ".json", timeout=5)
        response.raise_for_status()
        # Code here will only run if the request is successful
        response_info = response.json()
        return response_info
    except requests.exceptions.HTTPError as errh:
        return errh
    except requests.exceptions.ConnectionError as errc:
        return errc
    except requests.exceptions.Timeout as errt:
        return errt
    except requests.exceptions.RequestException as err:
        return err


def parse_multiple_target_attribute(response_info, attribute):
    if attribute == "publications":
        try:
            publications = response_info['properties']['PUBLICATIONS']['PUBLICATION']
            result = publications
        except Exception as e:
            # logger.error("Error: {}".format(e))
            result = None

    return result


def parse_target_study_publications(response_info):
    publications = parse_multiple_target_attribute(response_info, 'publications')

    return publications


def parse_studies_response(response_info):
    jga_studies = []
    results = response_info['dbXrefs']
    for result in results:
        type = result['type']
        if type == "jga-study":
            study_stable_id = result['identifier']
            jga_studies.append(study_stable_id)
    return jga_studies


""" DISCOVERS NEW JGA PUBLICATIONS USING THE JGAS ACCESSION"""


def discovery(conn_plsql):
    response_info = get_src_jga_studies()
    jga_studies = parse_studies_response(response_info)
    for study in jga_studies:
        #response_info = get_study(study)
        #existing_publications = parse_target_study_publications(response_info)
        #print(study, existing_publications)
        data = query_europubmed_simple(study)
        insert_study(data, conn_plsql)
        parse_europubmed_basic(data, conn_plsql, "DISCOVERY")


""" DISCOVERS NEW JGA PUBLICATIONS USING THE JGAD ACCESSION"""


def discovery_dataset(conn_plsql):
    jga_datasets = get_src_jga_datasets(conn_plsql)
    for dataset in jga_datasets:
        study = convert_dataset_study(conn_plsql, dataset)
        if study:
            data = query_europubmed_simple_dataset(study, str(dataset[0]))
            insert_study(data, conn_plsql)
            parse_europubmed_basic(data, conn_plsql, "DISCOVERY")


""" GET LINE 0 PUBMED """


def get_src_europubmed(conn_plsql):
    cursor = conn_plsql.cursor()
    sql = "SELECT DISTINCT  esa.article_id," \
          "  ar.cited_by" \
          "    FROM" \
          "    \"bibliography\".article ar " \
          "    INNER JOIN \"bibliography\".jga_study_article esa ON esa.article_id = ar.article_id" \
          "    order by cited_by ASC"
    cursor.execute(sql)
    records = cursor.fetchall()
    return records


""" INSERTS THE RELATION src_tgt PUBMED IN THE TABLE CITATION"""


def insert_src_tgt_pubmed(conn_plsql, src, tgt):
    cursor = conn_plsql.cursor()
    sql = "INSERT INTO \"bibliography\".citation (src_article_id,tgt_article_id)" + " SELECT %s , %s " \
          + " WHERE NOT EXISTS (SELECT src_article_id,tgt_article_id FROM \"bibliography\".citation WHERE src_article_id =%s  and tgt_article_id =%s);"
    cursor.execute(sql, (src, tgt, src, tgt))
    conn_plsql.commit()
    cursor.close()
    logger.info("Inserted citation " + str(src) + ":" + str(tgt))


""" CITATIONS -  MAIN METHOD"""


def citations(conn_plsql):
    src_europubmed = get_src_europubmed(conn_plsql)
    for pubmed_id in src_europubmed:
        if str(pubmed_id[1]) == "0":  # citations=0
            pass
        else:
            pubmed = str(pubmed_id[0])
            citations = str(pubmed_id[1])
            logger.info("pubmed " + str(pubmed) + " citations : " + str(citations) + "")
            data = query_europubmed_citations(pubmed)
            citations = parse_europubmed_citations(data, conn_plsql)
            for citation in citations:
                data = query_europubmed_id(pubmed, citation)
                parse_europubmed_basic(data, conn_plsql, "CITATION")


""" READ CONFIG FILE """


def read_config():
    with open("./config/config.yml", 'r') as ymlfile:
        cfg = yaml.safe_load(ymlfile)
    return cfg


""" MAIN BIBLIOGRAPHY METHOD"""


def bibliography():
    conn_plsql = None
    try:
        conn_plsql = connection_plsql()
        if conn_plsql:
            logger.info("#######LEGACY##########")
            # legacy_studies(conn_plsql)
            logger.info("#######XML##########")
            # xml_studies(conn_plsql)
            logger.info("#######DISCOVERY##########")
            discovery(conn_plsql)
            logger.info("#######DATASET##########")
            # discovery_dataset(conn_plsql)
            logger.info("#######CITATIONS##########")
            citations(conn_plsql)
    except psycopg2.DatabaseError as e:
        logger.warning("Error creating database:{} ".format(e))
        raise RuntimeError('Database error') from e
    finally:
        if conn_plsql:
            conn_plsql.close()
            logger.debug("PLSQL connection closed")


""" MAIN """


def main():
    try:
        # configure logging
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s [in %(pathname)s:%(lineno)d]'
        logging.basicConfig(format=log_format)
        logger.setLevel('DEBUG')
        global cfg
        cfg = read_config()
        # execute main function
        logger.info("Application Started")
        bibliography()
        logger.info("Application Ended")
    except Exception as e:
        logger.error("Error: {}".format(e))
        logger.debug(traceback.format_exc())
        sys.exit(-1)


if __name__ == '__main__':
    main()
