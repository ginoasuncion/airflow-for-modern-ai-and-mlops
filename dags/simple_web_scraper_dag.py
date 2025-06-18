"""
Simple Web Scraper DAG
A basic DAG that scrapes and parses data from a website
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import requests
from bs4 import BeautifulSoup
import pandas as pd
import json

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'simple_web_scraper',
    default_args=default_args,
    description='Simple web scraping and parsing DAG',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['web-scraping', 'simple'],
)

def scrape_quotes(**context):
    """Scrape quotes from quotes.toscrape.com"""
    try:
        url = "http://quotes.toscrape.com/"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        quotes = []
        
        # Find all quote elements
        quote_elements = soup.find_all('div', class_='quote')
        
        for quote_elem in quote_elements[:5]:  # Get first 5 quotes
            text_elem = quote_elem.find('span', class_='text')
            author_elem = quote_elem.find('small', class_='author')
            
            if text_elem and author_elem:
                quote_data = {
                    'text': text_elem.get_text().strip('"'),
                    'author': author_elem.get_text(),
                    'scraped_at': datetime.now().isoformat()
                }
                quotes.append(quote_data)
        
        # Save to JSON
        with open('/tmp/quotes.json', 'w') as f:
            json.dump(quotes, f, indent=2)
        
        print(f"Scraped {len(quotes)} quotes successfully")
        return f"Scraped {len(quotes)} quotes"
        
    except Exception as e:
        print(f"Error scraping quotes: {str(e)}")
        # Create mock data if scraping fails
        mock_quotes = [
            {'text': 'Be the change you wish to see in the world.', 'author': 'Mahatma Gandhi', 'scraped_at': datetime.now().isoformat()},
            {'text': 'Stay hungry, stay foolish.', 'author': 'Steve Jobs', 'scraped_at': datetime.now().isoformat()}
        ]
        with open('/tmp/quotes.json', 'w') as f:
            json.dump(mock_quotes, f, indent=2)
        return f"Used mock data: {len(mock_quotes)} quotes"

def parse_quotes(**context):
    """Parse the scraped quotes data"""
    try:
        # Load quotes from JSON
        with open('/tmp/quotes.json', 'r') as f:
            quotes = json.load(f)
        
        # Convert to DataFrame
        df = pd.DataFrame(quotes)
        
        # Add some analysis
        df['text_length'] = df['text'].str.len()
        df['word_count'] = df['text'].str.split().str.len()
        
        # Save to CSV
        df.to_csv('/tmp/quotes_parsed.csv', index=False)
        
        # Print summary
        print(f"Parsed {len(df)} quotes")
        print(f"Average text length: {df['text_length'].mean():.1f} characters")
        print(f"Average word count: {df['word_count'].mean():.1f} words")
        
        return f"Parsed {len(df)} quotes successfully"
        
    except Exception as e:
        print(f"Error parsing quotes: {str(e)}")
        raise

def generate_report(**context):
    """Generate a simple report"""
    try:
        # Load parsed data
        df = pd.read_csv('/tmp/quotes_parsed.csv')
        
        # Create report
        report = f"""
        Quote Analysis Report
        ====================
        
        Total quotes: {len(df)}
        Average text length: {df['text_length'].mean():.1f} characters
        Average word count: {df['word_count'].mean():.1f} words
        Longest quote: {df.loc[df['text_length'].idxmax(), 'text'][:50]}...
        Author with most quotes: {df['author'].mode().iloc[0] if not df['author'].mode().empty else 'N/A'}
        
        Generated at: {datetime.now()}
        """
        
        # Save report
        with open('/tmp/quote_report.txt', 'w') as f:
            f.write(report)
        
        print(report)
        return "Report generated successfully"
        
    except Exception as e:
        print(f"Error generating report: {str(e)}")
        raise

# Define tasks
start = EmptyOperator(
    task_id='start',
    dag=dag,
)

scrape_task = PythonOperator(
    task_id='scrape_quotes',
    python_callable=scrape_quotes,
    dag=dag,
)

parse_task = PythonOperator(
    task_id='parse_quotes',
    python_callable=parse_quotes,
    dag=dag,
)

report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag,
)

end = EmptyOperator(task_id='end', dag=dag)

# Define dependencies
start >> scrape_task >> parse_task >> report_task >> end 