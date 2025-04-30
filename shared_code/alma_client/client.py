"""Client for Ex Libris Alma APIs, specifically for Alma Analytics."""

import os
import requests
import xml.etree.ElementTree as ET
import logging
from typing import List, Dict, Tuple, Optional, Any, Union
import json


class AlmaClientError(Exception):
    """Base class for Alma Client errors."""

    pass


class AlmaApiError(AlmaClientError):
    """Exception raised for Alma API errors."""

    def __init__(self, status_code: int, error_code: str | None = None, error_message: str | None = None,
                 response_text: str | None = None):
        self.status_code = status_code
        self.error_code = error_code
        self.error_message = error_message
        self.response_text = response_text
        super().__init__(f"Alma API Error (Status: {status_code}): Code='{error_code}', Message='{error_message}'")


class AlmaClientConfigurationError(AlmaClientError):
    """Exception raised for configuration errors in Alma Client."""

    pass


XML_NAMESPACES = {
    'xsd': 'http://www.w3.org/2001/XMLSchema', 'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
    'rowset': 'urn:schemas-microsoft-com:xml-analysis:rowset', 'sql': 'urn:schemas-microsoft-com:xml-analysis:sql',
    'saw-sql': 'urn:saw-sql'
}


class AlmaClient:
    """
    Client for interacting with Ex Libris Alma APIs.
    Includes specific helpers for Analytics and a generic method for other APIs.
    """
    HEADING_REWRITE_RULES: List[Tuple[str, str]] = [
        (" CASE  WHEN Provenance Code = ", "Provenance Code"),
    ]

    def __init__(self, api_key: Optional[str] = None, base_url: Optional[str] = None):
        """ Initializes the AlmaClient. Prioritizes passed arguments. """
        self.api_key = api_key
        if not self.api_key:
            logging.debug("API key not provided directly, checking ALMA_API_KEY env var.")
            self.api_key = os.environ.get('ALMA_API_KEY')

        _raw_base_url = base_url or os.environ.get('ALMA_API_BASE_URL')

        if not self.api_key:
            raise AlmaClientConfigurationError("Alma API Key is required.")
        if not _raw_base_url:
            raise AlmaClientConfigurationError("Alma API Base URL is required.")

        self.base_url = _raw_base_url.rstrip('/')
        # Default headers can be overridden by specific methods or make_api_call
        self.default_headers = {'Authorization': f'apikey {self.api_key}'}
        logging.info(f"AlmaClient initialized for base URL: {self.base_url}")

    # **** MODIFIED: Made slightly more generic ****
    def _make_request(
            self,
            method: str,
            api_path: str,
            params: Optional[Dict] = None,
            headers: Optional[Dict] = None,  # Allow overriding default headers
            json_payload: Optional[Any] = None,  # For sending JSON body
            data_payload: Optional[Union[str, bytes]] = None,  # For sending form data or raw body
            timeout: int = 270
    ) -> requests.Response:
        """ Internal helper method to make HTTP requests. """
        url = f"{self.base_url}{api_path}"
        # Start with default, allow method-specific headers to override/add
        request_headers = self.default_headers.copy()
        if headers:
            request_headers.update(headers)

        # Determine content type if body is present and not already set
        if (json_payload is not None or data_payload is not None) and 'Content-Type' not in request_headers:
            if json_payload is not None:
                request_headers['Content-Type'] = 'application/json'
            # Add other types like application/xml if needed based on data_payload type

        logging.debug(f"Making Alma API request: {method} {url}")
        logging.debug(f"Params: {params}")
        logging.debug(f"Headers: {request_headers}")
        if json_payload:
            logging.debug(f"JSON Body: {str(json_payload)[:200]}...")  # Log truncated body
        if data_payload:
            logging.debug(f"Data Body: {str(data_payload)[:200]}...")  # Log truncated body

        try:
            response = requests.request(
                method=method,
                url=url,
                headers=request_headers,
                params=params,
                json=json_payload,  # Use requests' json param for auto-serialization
                data=data_payload,  # Use data for pre-encoded strings/bytes
                timeout=timeout
            )
            logging.debug(f"Response Status Code: {response.status_code}")
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as http_err:
            # ... (keep existing detailed error parsing) ...
            logging.error(f"HTTP error occurred: {http_err} - Response: {http_err.response.text[:500]}...")
            error_code = None
            error_message = None
            try:
                if 'application/xml' in http_err.response.headers.get('Content-Type', ''):
                    error_root = ET.fromstring(http_err.response.text)
                    err_ns = {'error': 'http://com/exlibris/urm/general/xmlbeans'}
                    code_elem = error_root.find('.//error:errorCode', err_ns) or error_root.find('.//errorCode')
                    msg_elem = (error_root.find('.//error:errorMessage', err_ns) or
                                error_root.find('.//errorMessage'))
                    if code_elem is not None:
                        error_code = code_elem.text
                    if msg_elem is not None:
                        error_message = msg_elem.text
                # Add JSON error parsing maybe? Often error format is consistent though.
                # elif 'application/json' in http_err.response.headers.get('Content-Type', ''): ...
            except Exception as parse_err:
                logging.warning(f"Could not parse Alma error details: {parse_err}")
            raise AlmaApiError(status_code=http_err.response.status_code, error_code=error_code,
                               error_message=error_message, response_text=http_err.response.text) from http_err
        except requests.exceptions.RequestException as req_err:
            logging.error(f"Request exception occurred: {req_err}")
            raise  # Re-raise other request exceptions

    # **** ADDED: Generic Public Method ****
    def make_api_call(
            self,
            method: str,
            api_path: str,
            path_params: Optional[Dict[str, Any]] = None,
            query_params: Optional[Dict[str, Any]] = None,
            json_body: Optional[Any] = None,
            data_body: Optional[Union[str, bytes]] = None,
            extra_headers: Optional[Dict[str, str]] = None
    ) -> requests.Response:
        """
        Makes a generic call to the Alma API.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE).
            api_path: Relative API path (e.g., '/bibs/{mms_id}/holdings').
                      Use curly braces {} for path parameters.
            path_params: Dictionary to format into the api_path string.
            query_params: Dictionary of query string parameters.
            json_body: Python object to be sent as JSON request body.
            data_body: String or bytes to be sent as raw request body.
            extra_headers: Dictionary of extra headers to send (e.g., Accept, Content-Type).
                           These merge with/override defaults.

        Returns:
            The raw requests.Response object. Caller handles parsing.

        Raises:
            AlmaApiError: If the API returns an error status code.
            requests.exceptions.RequestException: For network issues.
            ValueError: If path formatting fails.
        """
        formatted_path = api_path
        if path_params:
            try:
                formatted_path = api_path.format(**path_params)
            except KeyError as e:
                raise ValueError(f"Missing key for path formatting in api_path '{api_path}': {e}") from e

        # Prepare headers, ensure Accept is set if not provided (default to JSON for generic calls?)
        headers = {'Accept': 'application/json'}  # Default for non-Analytics generic calls
        if extra_headers:
            headers.update(extra_headers)

        return self._make_request(
            method=method.upper(),
            api_path=formatted_path,
            params=query_params,
            headers=headers,  # Pass combined headers
            json_payload=json_body,
            data_payload=data_body
        )

    # **** END ADDITION ****

    def _rewrite_heading(self, original_heading: str) -> str:
        # ... (no changes in _rewrite_heading) ...
        if not original_heading:
            return ""
        for find_str, replace_with in self.HEADING_REWRITE_RULES:
            if find_str in original_heading:
                logging.debug(f"Rewriting heading '{original_heading}' to '{replace_with}'")
                return replace_with
        return original_heading

    def get_analytics_report_chunk(
            self,
            report_path: Optional[str] = None,
            limit: int = 1000,
            resumption_token: Optional[str] = None,
            header_map: Optional[Dict[str, str]] = None
    ) -> Tuple[List[Dict[str, Optional[str]]], Optional[str], bool, Optional[Dict[str, str]]]:
        """ Fetches Analytics report chunk using rewritten column headings. """
        # ... (Input validation remains the same) ...
        if not resumption_token and not report_path:
            raise ValueError("report_path required if no token.")
        if not 1 <= limit <= 1000:
            raise ValueError("Limit must be between 1 and 1000.")

        api_path = "/almaws/v1/analytics/reports"
        params = {}
        # **** ENSURE col_headings is ONLY set for initial call ****
        if resumption_token:
            params['token'] = resumption_token
            logging.info(f"Fetching Analytics report chunk using token.")
        else:
            params['path'] = report_path
            params['limit'] = limit
            params['col_headings'] = 'true'  # Correctly only added here
            logging.info(f"Fetching INITIAL Analytics chunk. Path: '{report_path}'. Requesting headings.")

        # Use _make_request, ensuring correct Accept header for Analytics XML
        analytics_headers = {'Accept': 'application/xml'}
        response = self._make_request('GET', api_path, params=params, headers=analytics_headers)
        # **** END Ensure col_headings ****

        try:
            root = ET.fromstring(response.content)
            # ... (rest of XML parsing, header mapping, row extraction logic remains the same as previous version) ...
            # ... This includes extracting token, is_finished, parsing schema if needed, ...
            # ... building active_header_map, ordered_column_names, ...
            # ... iterating through rows, creating row_dict with final headings as keys. ...

            # --- (Copying the parsing logic from previous correct answer for completeness) ---
            token_element = root.find('.//ResumptionToken')
            next_token = token_element.text if token_element is not None and token_element.text else None
            finished_element = root.find('.//IsFinished')
            is_finished = finished_element is not None and finished_element.text.lower() == 'true'
            logging.info(f"IsFinished flag: {is_finished}, Next Token Present: {next_token is not None}")

            active_header_map: Dict[str, str] = {}
            header_map_found_this_call: Optional[Dict[str, str]] = None
            ordered_column_names: List[str] = []

            if header_map is not None:
                active_header_map = header_map
                ordered_column_names = list(active_header_map.keys())
                logging.debug(f"Using known header map. Order assumed: {ordered_column_names}")
            elif resumption_token is None:
                logging.debug("Initial call, attempting to parse schema for names and headings.")
                schema_path = ('.//ResultXml/rowset:rowset/xsd:schema/xsd:complexType[@name="Row"]/xsd:sequence/'
                               'xsd:element')
                schema_elements = root.findall(schema_path, XML_NAMESPACES)
                if schema_elements:
                    parsed_map = {}
                    temp_ordered_names = []
                    heading_attr = f"{{{XML_NAMESPACES['saw-sql']}}}columnHeading"
                    for elem in schema_elements:
                        col_name = elem.get('name')
                        if not col_name:
                            continue
                        original_heading = elem.get(heading_attr, col_name)
                        original_heading = original_heading or col_name
                        final_heading = self._rewrite_heading(original_heading)
                        parsed_map[col_name] = final_heading
                        temp_ordered_names.append(col_name)
                    if parsed_map:
                        active_header_map = parsed_map
                        header_map_found_this_call = parsed_map
                        ordered_column_names = temp_ordered_names
                        logging.info(f"Extracted {len(active_header_map)} column names/headings from schema.")
                        logging.debug(
                            f"DEBUG Client: Parsed Header Map (ColX -> Heading): {header_map_found_this_call}")
                        logging.debug(f"DEBUG Client: Column Name Order (ColX): {ordered_column_names}")
                    else:
                        logging.warning("Schema elements found but failed extraction. Using generic 'ColumnX'.")
                else:
                    logging.warning("No schema definition found. Using generic 'ColumnX' names.")
            else:
                logging.error("Subsequent call but no header_map provided. Using generic 'ColumnX' names.")

            rows_data: List[Dict[str, Optional[str]]] = []
            rowset_path = './/ResultXml/rowset:rowset'
            rowset_element = root.find(rowset_path, XML_NAMESPACES)
            if rowset_element is None:
                rowset_element = root.find('.//rowset:rowset', XML_NAMESPACES)

            if rowset_element is not None:
                row_elements = rowset_element.findall('rowset:Row', XML_NAMESPACES)
                logging.debug(f"Found {len(row_elements)} Row elements.")
                expected_headings_in_order = [active_header_map.get(col_name, col_name) for col_name in
                                              ordered_column_names] if ordered_column_names else []
                for row_elem in row_elements:
                    row_dict: Dict[str, Optional[str]] = {heading: None for heading in expected_headings_in_order}
                    cols_in_row_by_local_name = {col_elem.tag.split('}')[-1]: col_elem.text for col_elem in
                                                 list(row_elem)}
                    for i, col_name in enumerate(ordered_column_names):
                        final_heading = active_header_map.get(col_name)
                        if final_heading:
                            if col_name in cols_in_row_by_local_name:
                                row_dict[final_heading] = cols_in_row_by_local_name[col_name]
                        else:
                            if col_name in cols_in_row_by_local_name:
                                logging.warning(
                                    f"Using fallback key '{col_name}'")
                                row_dict[col_name] = cols_in_row_by_local_name[
                                    col_name]
                    rows_data.append(row_dict)
            else:
                logging.warning("Could not find <rowset:rowset> element.")
            # --- End copied parsing logic ---

            logging.info(f"Processed {len(rows_data)} rows for this chunk.")
            return rows_data, next_token, is_finished, header_map_found_this_call

        except ET.ParseError as parse_err:
            logging.error(f"Failed to parse XML: {parse_err}", exc_info=True)
            raise
        except Exception as e:
            logging.error(f"Unexpected error processing response: {e}", exc_info=True)
            raise AlmaClientError(
                f"Failed processing response: {e}") from e
