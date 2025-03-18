import html

import pandas as pd
import requests
import xmltodict
import json
import os

from loguru import logger
from src.config import ENV_ROOT

class dtx:
    """
    Class to make requests to the RSA web service.
    """
    def __init__(self):        
        # Get credentials from environment variables
        self.username = os.environ.get("RSA_USERNAME")
        self.password = os.environ.get("RSA_PASSWORD")
        
        # Validate that credentials are available
        if not self.username or not self.password:
            logger.error("RSA credentials not found in environment variables")
            raise ValueError("RSA_USERNAME and RSA_PASSWORD must be set in .env file")

        self.url = "https://salut.gencat.cat/rsa/AppJava/services/SpringWS"
        self.req_headers = {"Content-Type": "text/xml; charset=utf-8", "SOAPAction": ""}
    
    def _ping(self) -> bool:
        """
        Pings the RSA web service to check if it's available.

        Returns:
            bool: True if the server responds with 'OK', False otherwise
        """
        # Create the SOAP ping request
        req_body = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:rsaw="http://rsaws">
        <soapenv:Header/>
        <soapenv:Body>
        <rsaw:ping/>
        </soapenv:Body>
        </soapenv:Envelope>"""

        # Send the request
        response = requests.post(
            self.url,
            data=req_body,
            headers=self.req_headers,
            timeout=30
        )

        # Parse the response
        parsed_data = xmltodict.parse(
            html.unescape(response.content.decode(response.apparent_encoding))
        )

        # Check for the 'OK' response
        if (parsed_data.get("soapenv:Envelope", {})
                    .get("soapenv:Body", {})
                    .get("pingResponse", {})
                    .get("pingReturn") == "OK"):
            return True
        return False

    def download_data(self, id_query, start_date, end_date, columns) -> pd.DataFrame:
        """
        Makes a request to the RSA web service and returns the parsed data.
        """
        try: # Try to make the request, if server works, parse the response
            req_body = f"""<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
                    xmlns:rsaw="http://rsaws">
                    <soapenv:Header/>
                    <soapenv:Body>
                    <rsaw:consulta>
                    <dadesConsulta>
                    <idConsulta>{id_query}</idConsulta>
                    <idRegistre>567</idRegistre>
                    <parametre valor1="{start_date}" valor2="{end_date}" xmlns="" />
                    </dadesConsulta>
                    <usuari>{self.username}</usuari>
                    <pwd>{self.password}</pwd>
                    <aplicacio>DTX</aplicacio>
                    </rsaw:consulta>
                    </soapenv:Body>
                    </soapenv:Envelope>
                    """
            response = requests.post(self.url, data=req_body, headers=self.req_headers, timeout=100)
            parsed_data = xmltodict.parse(html.unescape(response.content.decode(response.apparent_encoding)))

            ocurrencias = parsed_data["soapenv:Envelope"]["soapenv:Body"]["consultaResponse"]["consultaReturn"].get("varOcurrencia", [])
            ocurrencias = [ocurrencias] if isinstance(ocurrencias, dict) else ocurrencias  # Ensure it's always a list

            if not ocurrencias:
                error_msg = parsed_data["soapenv:Envelope"]["soapenv:Body"]["consultaResponse"]["consultaReturn"].get("error", {}).get("missatge")
                if error_msg:
                    logger.warn(f"Error making the query: {error_msg}")
                else:
                    logger.warn("No data found for this query")
                return None

            # Create a list of dictionaries with the data (one row per donor, with codes as column names)
            rows = [
                {
                    f"{var['attribute']['value']['@nrs_cnrs']}_{var['attribute']['value']['@vnr_cvnr']}":
                    (var["@document"] if id_query == "624505" and f"{var['attribute']['value']['@nrs_cnrs']}_{var['attribute']['value']['@vnr_cvnr']}" == "1_1684"
                    else var["descripcio"])
                    for var in ocurrencia["varArray"]
                }
                for ocurrencia in ocurrencias
            ]
            
            df = pd.DataFrame(rows)
            # Create new variable id_donant (primary key for the donor DB)
            df["id_donant"] = df["1_881"] + "_" + df["1_1515"]
            # Rename columns with mapping
            df.rename(columns = columns, inplace=True)
            return df

        except Exception:
            print("Error desconocido")
            return None

    def create_donor(self, variables: dict[str, str]) -> dict:
        """
        Creates a new donor in the RSA web service.

        Args:
            tipus_donant (str): Type of donor
            data_donacio (str): Donation date
            sexe (str): Gender
            edat (str): Age
            nom (str): Name
            cognoms (str): Surnames

        Returns:
            dict: Parsed response from the web service

        Info:
            PK: [Nom, Primer Cognom, Segon Cognom, Data Naixement, Data Donació]
        """
        varArray = [f"<varArray codi=\"{k}\" valor=\"{v}\"></varArray>" for k, v in attributes.items()]

        req_body = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:rsaw="http://rsaws">
                    <soapenv:Header/>
                    <soapenv:Body>
                    <rsaw:alta>
                    <registre>
                    <varOcurrencies>
                    """ + "\n".join(varArray) + """<nivell>1</nivell>
                    </varOcurrencies>
                    <idRegistre>567</idRegistre>
                    </registre>
                    <usuari>{self.username}</usuari>
                    <pwd>{self.password}</pwd>
                    <aplicacio>DTX</aplicacio>
                    </rsaw:alta>
                    </soapenv:Body>
                    </soapenv:Envelope>"""
        print(req_body)
        try:
            response = requests.post(self.url, data=req_body, headers=self.req_headers, timeout=100)
            parsed_data = xmltodict.parse(html.unescape(response.content.decode(response.apparent_encoding)))
            
            # Save response for debugging
            with open("response.json", "w") as f:
                json.dump(parsed_data, f)
            
            # Extract the response path
            alta_return = parsed_data.get("soapenv:Envelope", {}).get("soapenv:Body", {}).get("altaResponse", {}).get("altaReturn", {})
            
            # Check if there's an error in the response
            if alta_return.get("error") is not None:
                error_msg = alta_return.get("error", {}).get("missatge", "Unknown error")
                print(f"Error creating donor: {error_msg}")
                return error_msg
            
            # Check for varOcurrencies which contains the idOcurrencia
            var_ocurrencies = alta_return.get("varOcurrencies", {})
            id_ocurrencia = parsed_data["soapenv:Envelope"]["soapenv:Body"]["altaResponse"]["altaReturn"]["varOcurrencies"]["idOcurrencia"]
            logger.info(f"Donor created successfully with idOcurrencia: {id_ocurrencia}")
            
        except Exception as e:
            error_message = f"Exception in create_donor: {str(e)}"
            print(error_message)
            import traceback
            traceback.print_exc()
            return error_message

    def modify(self, modifications: dict[str, str]) -> dict:
        """
        Modifies a donor in the RSA web service.

        Args:
            id_donant (str): Donor ID
        """
        varArray = [f"<varArray codi=\"{k}\" valor=\"{v}\"></varArray>" for k, v in attributes.items()]

        req_body = """<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:rsaw="http://rsaws">"
                    <soapenv:Header/>
                    <soapenv:Body>
                    <rsaw:modificacio>
                    <registre>""" + "\n".join(varArray) + """
                    <idRegistre>567</idRegistre>
                    <nivell>1</nivell>
                    <idOcurrencia>631477113</idOcurrencia>
                    <idPk>-1</idPk>
                    </registre>
                    <usuari>{self.username}</usuari>
                    <pwd>{self.password}</pwd>
                    <aplicacio>DTX</aplicacio>
                    </rsaw:modificacio>
                    </soapenv:Body>
                    </soapenv:Envelope>"""

        response = requests.post(self.url, data=req_body, headers=self.req_headers, timeout=100)
        parsed_data = xmltodict.parse(html.unescape(response.content.decode(response.apparent_encoding)))

        return parsed_data

    def eliminate(self, id_ocurrencia: str) -> dict:
        req_body = f"""<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:rsaw="http://rsaws">
                        <soapenv:Header/>
                        <soapenv:Body>
                        <rsaw:eliminacio>
                        <registre>
                        <idRegistre>567</idRegistre>
                        <nivell>1</nivell>
                        <idOcurrencia>{id_ocurrencia}</idOcurrencia>
                        <idPk>-1</idPk>
                        </registre>
                        <usuari>{self.username}</usuari>
                        <pwd>{self.password}</pwd>
                        <aplicacio>DTX</aplicacio>
                        </rsaw:eliminacio>
                        </soapenv:Body>
                        </soapenv:Envelope>
                    """
        response = requests.post(self.url, data=req_body, headers=self.req_headers, timeout=100)
        parsed_data = xmltodict.parse(html.unescape(response.content.decode(response.apparent_encoding)))
        with open("response.json", "w") as f:
            json.dump(parsed_data, f)
        print(parsed_data)
        return parsed_data