"""
Data Parsing Functions
Reusable functions for parsing different types of data from websites
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import re
from datetime import datetime
import logging
from typing import List, Dict, Any, Optional

def parse_html_table(url: str, table_selector: str = "table") -> pd.DataFrame:
    """
    Parse HTML table from a website
    
    Args:
        url: Website URL to scrape
        table_selector: CSS selector for the table (default: "table")
    
    Returns:
        DataFrame containing the table data
    """
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.select_one(table_selector)
        
        if not table:
            raise ValueError(f"No table found with selector: {table_selector}")
        
        # Extract headers
        headers = []
        header_row = table.find('thead')
        if header_row:
            headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]
        else:
            # Try to get headers from first row
            first_row = table.find('tr')
            if first_row:
                headers = [th.get_text(strip=True) for th in first_row.find_all(['th', 'td'])]
        
        # Extract data rows
        rows = []
        data_rows = table.find_all('tr')[1:] if header_row else table.find_all('tr')
        
        for row in data_rows:
            cells = row.find_all(['td', 'th'])
            if cells:
                row_data = [cell.get_text(strip=True) for cell in cells]
                rows.append(row_data)
        
        # Create DataFrame
        df = pd.DataFrame(rows, columns=headers)
        return df
        
    except Exception as e:
        logging.error(f"Error parsing HTML table from {url}: {str(e)}")
        raise

def parse_json_api(url: str, params: Optional[Dict] = None) -> pd.DataFrame:
    """
    Parse JSON data from an API endpoint
    
    Args:
        url: API endpoint URL
        params: Query parameters for the API request
    
    Returns:
        DataFrame containing the JSON data
    """
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        # Handle different JSON structures
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, dict):
            # If it's a dict with a data key, extract that
            if 'data' in data:
                df = pd.DataFrame(data['data'])
            elif 'results' in data:
                df = pd.DataFrame(data['results'])
            else:
                # Convert single dict to DataFrame
                df = pd.DataFrame([data])
        else:
            raise ValueError(f"Unexpected JSON structure: {type(data)}")
        
        return df
        
    except Exception as e:
        logging.error(f"Error parsing JSON from {url}: {str(e)}")
        raise

def parse_csv_from_url(url: str, **kwargs) -> pd.DataFrame:
    """
    Parse CSV data from a URL
    
    Args:
        url: URL pointing to CSV file
        **kwargs: Additional arguments to pass to pd.read_csv
    
    Returns:
        DataFrame containing the CSV data
    """
    try:
        df = pd.read_csv(url, **kwargs)
        return df
        
    except Exception as e:
        logging.error(f"Error parsing CSV from {url}: {str(e)}")
        raise

def parse_xml_data(url: str, root_element: str = None) -> pd.DataFrame:
    """
    Parse XML data from a website
    
    Args:
        url: Website URL to scrape
        root_element: XML root element to parse (optional)
    
    Returns:
        DataFrame containing the XML data
    """
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'xml')
        
        # Find all items (common XML patterns)
        items = soup.find_all(['item', 'entry', 'record'])
        
        if not items:
            # Try alternative patterns
            items = soup.find_all(['div', 'article', 'product'])
        
        data = []
        for item in items:
            item_data = {}
            for child in item.children:
                if hasattr(child, 'name') and child.name:
                    item_data[child.name] = child.get_text(strip=True)
            if item_data:
                data.append(item_data)
        
        if not data:
            raise ValueError("No parsable XML data found")
        
        df = pd.DataFrame(data)
        return df
        
    except Exception as e:
        logging.error(f"Error parsing XML from {url}: {str(e)}")
        raise

def parse_text_content(url: str, text_selectors: List[str] = None) -> Dict[str, str]:
    """
    Parse text content from a website using CSS selectors
    
    Args:
        url: Website URL to scrape
        text_selectors: List of CSS selectors for text content
    
    Returns:
        Dictionary containing extracted text content
    """
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        if not text_selectors:
            # Default selectors for common content
            text_selectors = ['h1', 'h2', 'h3', 'p', '.content', '.article']
        
        content = {}
        for selector in text_selectors:
            elements = soup.select(selector)
            if elements:
                content[selector] = [elem.get_text(strip=True) for elem in elements]
        
        return content
        
    except Exception as e:
        logging.error(f"Error parsing text content from {url}: {str(e)}")
        raise

def clean_and_transform_data(df: pd.DataFrame, cleaning_rules: Dict[str, Any] = None) -> pd.DataFrame:
    """
    Clean and transform parsed data
    
    Args:
        df: Input DataFrame
        cleaning_rules: Dictionary of cleaning rules
    
    Returns:
        Cleaned DataFrame
    """
    try:
        df_cleaned = df.copy()
        
        # Remove duplicate rows
        df_cleaned = df_cleaned.drop_duplicates()
        
        # Remove rows with all NaN values
        df_cleaned = df_cleaned.dropna(how='all')
        
        # Apply custom cleaning rules
        if cleaning_rules:
            for column, rule in cleaning_rules.items():
                if column in df_cleaned.columns:
                    if rule.get('type') == 'strip':
                        df_cleaned[column] = df_cleaned[column].astype(str).str.strip()
                    elif rule.get('type') == 'lowercase':
                        df_cleaned[column] = df_cleaned[column].astype(str).str.lower()
                    elif rule.get('type') == 'uppercase':
                        df_cleaned[column] = df_cleaned[column].astype(str).str.upper()
                    elif rule.get('type') == 'replace':
                        df_cleaned[column] = df_cleaned[column].replace(rule.get('mapping', {}))
        
        # Add metadata
        df_cleaned['parsed_at'] = datetime.now().isoformat()
        df_cleaned['row_count'] = len(df_cleaned)
        
        return df_cleaned
        
    except Exception as e:
        logging.error(f"Error cleaning data: {str(e)}")
        raise

def extract_structured_data(url: str, data_type: str = 'auto') -> pd.DataFrame:
    """
    Automatically detect and extract structured data from a website
    
    Args:
        url: Website URL to scrape
        data_type: Type of data to extract ('auto', 'table', 'json', 'xml')
    
    Returns:
        DataFrame containing the extracted data
    """
    try:
        if data_type == 'auto':
            # Try different parsing methods
            try:
                return parse_html_table(url)
            except:
                try:
                    return parse_json_api(url)
                except:
                    try:
                        return parse_xml_data(url)
                    except:
                        raise ValueError("Could not automatically detect data structure")
        
        elif data_type == 'table':
            return parse_html_table(url)
        elif data_type == 'json':
            return parse_json_api(url)
        elif data_type == 'xml':
            return parse_xml_data(url)
        else:
            raise ValueError(f"Unsupported data type: {data_type}")
            
    except Exception as e:
        logging.error(f"Error extracting structured data from {url}: {str(e)}")
        raise

def save_parsed_data(df: pd.DataFrame, output_path: str, format: str = 'csv') -> str:
    """
    Save parsed data to file
    
    Args:
        df: DataFrame to save
        output_path: Output file path
        format: Output format ('csv', 'json', 'parquet', 'excel')
    
    Returns:
        Path to saved file
    """
    try:
        if format == 'csv':
            df.to_csv(output_path, index=False)
        elif format == 'json':
            df.to_json(output_path, orient='records', indent=2)
        elif format == 'parquet':
            df.to_parquet(output_path, index=False)
        elif format == 'excel':
            df.to_excel(output_path, index=False)
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        logging.info(f"Data saved to {output_path}")
        return output_path
        
    except Exception as e:
        logging.error(f"Error saving data to {output_path}: {str(e)}")
        raise 