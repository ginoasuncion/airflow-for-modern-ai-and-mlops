"""
Web Scraping DAG
A DAG that demonstrates web scraping and data parsing with Airflow
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import logging

# Default arguments for the DAG
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
    'web_scraping_dag',
    default_args=default_args,
    description='A DAG that scrapes and parses data from websites',
    schedule_interval=timedelta(hours=6),  # Run every 6 hours
    catchup=False,
    tags=['web-scraping', 'data-parsing', 'etl'],
)

def scrape_news_headlines(**context):
    """
    Scrape news headlines from a news website
    """
    try:
        # Using a public API for news headlines (more reliable than scraping)
        url = "https://newsapi.org/v2/top-headlines"
        params = {
            'country': 'us',
            'apiKey': 'demo_key'  # Replace with actual API key
        }
        
        # For demo purposes, we'll create mock data
        mock_headlines = [
            {
                'title': 'Tech Innovation Drives Market Growth',
                'description': 'New technologies are transforming industries worldwide',
                'source': 'Tech News',
                'published_at': datetime.now().isoformat(),
                'category': 'Technology'
            },
            {
                'title': 'AI Breakthrough in Healthcare',
                'description': 'Artificial intelligence shows promising results in medical diagnosis',
                'source': 'Health Daily',
                'published_at': datetime.now().isoformat(),
                'category': 'Healthcare'
            },
            {
                'title': 'Climate Change Summit Results',
                'description': 'Global leaders agree on new environmental policies',
                'source': 'World News',
                'published_at': datetime.now().isoformat(),
                'category': 'Environment'
            }
        ]
        
        # Convert to DataFrame
        df = pd.DataFrame(mock_headlines)
        
        # Save to JSON for next task
        df.to_json('/tmp/news_headlines.json', orient='records', indent=2)
        
        logging.info(f"Scraped {len(df)} news headlines")
        return f"Successfully scraped {len(df)} headlines"
        
    except Exception as e:
        logging.error(f"Error scraping news: {str(e)}")
        raise

def scrape_weather_data(**context):
    """
    Scrape weather data (mock implementation)
    """
    try:
        # Mock weather data
        weather_data = [
            {
                'city': 'New York',
                'temperature': 72,
                'condition': 'Sunny',
                'humidity': 65,
                'timestamp': datetime.now().isoformat()
            },
            {
                'city': 'Los Angeles',
                'temperature': 78,
                'condition': 'Partly Cloudy',
                'humidity': 55,
                'timestamp': datetime.now().isoformat()
            },
            {
                'city': 'Chicago',
                'temperature': 68,
                'condition': 'Rainy',
                'humidity': 80,
                'timestamp': datetime.now().isoformat()
            }
        ]
        
        df = pd.DataFrame(weather_data)
        df.to_json('/tmp/weather_data.json', orient='records', indent=2)
        
        logging.info(f"Scraped weather data for {len(df)} cities")
        return f"Successfully scraped weather data for {len(df)} cities"
        
    except Exception as e:
        logging.error(f"Error scraping weather: {str(e)}")
        raise

def parse_and_clean_data(**context):
    """
    Parse and clean the scraped data
    """
    try:
        # Load news headlines
        news_df = pd.read_json('/tmp/news_headlines.json')
        
        # Load weather data
        weather_df = pd.read_json('/tmp/weather_data.json')
        
        # Clean news data
        news_df['title_length'] = news_df['title'].str.len()
        news_df['word_count'] = news_df['title'].str.split().str.len()
        news_df['scraped_at'] = datetime.now().isoformat()
        
        # Clean weather data
        weather_df['temperature_f'] = weather_df['temperature']
        weather_df['temperature_c'] = (weather_df['temperature'] - 32) * 5/9
        weather_df['scraped_at'] = datetime.now().isoformat()
        
        # Save cleaned data
        news_df.to_csv('/tmp/cleaned_news.csv', index=False)
        weather_df.to_csv('/tmp/cleaned_weather.csv', index=False)
        
        # Create summary statistics
        summary = {
            'news_count': len(news_df),
            'weather_cities': len(weather_df),
            'avg_temperature': weather_df['temperature'].mean(),
            'categories': news_df['category'].value_counts().to_dict()
        }
        
        with open('/tmp/data_summary.json', 'w') as f:
            json.dump(summary, f, indent=2)
        
        logging.info(f"Parsed and cleaned data. Summary: {summary}")
        return f"Successfully parsed data. News: {len(news_df)}, Weather: {len(weather_df)}"
        
    except Exception as e:
        logging.error(f"Error parsing data: {str(e)}")
        raise

def analyze_data(**context):
    """
    Perform data analysis on the cleaned data
    """
    try:
        # Load cleaned data
        news_df = pd.read_csv('/tmp/cleaned_news.csv')
        weather_df = pd.read_csv('/tmp/cleaned_weather.csv')
        
        # Analysis results
        analysis_results = {
            'news_analysis': {
                'total_headlines': len(news_df),
                'avg_title_length': news_df['title_length'].mean(),
                'avg_word_count': news_df['word_count'].mean(),
                'category_distribution': news_df['category'].value_counts().to_dict()
            },
            'weather_analysis': {
                'total_cities': len(weather_df),
                'avg_temperature_f': weather_df['temperature_f'].mean(),
                'avg_temperature_c': weather_df['temperature_c'].mean(),
                'temperature_range': {
                    'min': weather_df['temperature_f'].min(),
                    'max': weather_df['temperature_f'].max()
                },
                'condition_distribution': weather_df['condition'].value_counts().to_dict()
            }
        }
        
        # Save analysis results
        with open('/tmp/analysis_results.json', 'w') as f:
            json.dump(analysis_results, f, indent=2)
        
        # Create a summary report
        report = f"""
        Data Analysis Report
        ===================
        
        News Data:
        - Total headlines: {analysis_results['news_analysis']['total_headlines']}
        - Average title length: {analysis_results['news_analysis']['avg_title_length']:.1f} characters
        - Average word count: {analysis_results['news_analysis']['avg_word_count']:.1f} words
        
        Weather Data:
        - Total cities: {analysis_results['weather_analysis']['total_cities']}
        - Average temperature: {analysis_results['weather_analysis']['avg_temperature_f']:.1f}°F ({analysis_results['weather_analysis']['avg_temperature_c']:.1f}°C)
        - Temperature range: {analysis_results['weather_analysis']['temperature_range']['min']}°F - {analysis_results['weather_analysis']['temperature_range']['max']}°F
        """
        
        with open('/tmp/analysis_report.txt', 'w') as f:
            f.write(report)
        
        logging.info("Data analysis completed successfully")
        return "Data analysis completed successfully"
        
    except Exception as e:
        logging.error(f"Error analyzing data: {str(e)}")
        raise

def scrape_real_website_data(**context):
    """
    Scrape data from a real website (example: quotes website)
    """
    try:
        # Using a quotes website for demonstration
        url = "http://quotes.toscrape.com/"
        
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        quotes = []
        quote_elements = soup.find_all('div', class_='quote')
        
        for quote_elem in quote_elements[:10]:  # Limit to 10 quotes
            text_elem = quote_elem.find('span', class_='text')
            author_elem = quote_elem.find('small', class_='author')
            tags_elem = quote_elem.find_all('a', class_='tag')
            
            if text_elem and author_elem:
                quote_data = {
                    'text': text_elem.get_text().strip('"'),
                    'author': author_elem.get_text(),
                    'tags': [tag.get_text() for tag in tags_elem],
                    'scraped_at': datetime.now().isoformat()
                }
                quotes.append(quote_data)
        
        # Convert to DataFrame
        df = pd.DataFrame(quotes)
        
        # Save to JSON
        df.to_json('/tmp/quotes_data.json', orient='records', indent=2)
        
        logging.info(f"Successfully scraped {len(df)} quotes from {url}")
        return f"Successfully scraped {len(df)} quotes"
        
    except Exception as e:
        logging.error(f"Error scraping quotes: {str(e)}")
        # Return mock data if scraping fails
        mock_quotes = [
            {
                'text': 'Be the change you wish to see in the world.',
                'author': 'Mahatma Gandhi',
                'tags': ['inspirational', 'change'],
                'scraped_at': datetime.now().isoformat()
            },
            {
                'text': 'Stay hungry, stay foolish.',
                'author': 'Steve Jobs',
                'tags': ['inspirational', 'technology'],
                'scraped_at': datetime.now().isoformat()
            }
        ]
        
        df = pd.DataFrame(mock_quotes)
        df.to_json('/tmp/quotes_data.json', orient='records', indent=2)
        
        return f"Used mock data due to scraping error. {len(df)} quotes available"

# Define tasks
start = DummyOperator(task_id='start', dag=dag)

scrape_news = PythonOperator(
    task_id='scrape_news_headlines',
    python_callable=scrape_news_headlines,
    dag=dag,
)

scrape_weather = PythonOperator(
    task_id='scrape_weather_data',
    python_callable=scrape_weather_data,
    dag=dag,
)

scrape_quotes = PythonOperator(
    task_id='scrape_real_website_data',
    python_callable=scrape_real_website_data,
    dag=dag,
)

parse_data = PythonOperator(
    task_id='parse_and_clean_data',
    python_callable=parse_and_clean_data,
    dag=dag,
)

analyze_data_task = PythonOperator(
    task_id='analyze_data',
    python_callable=analyze_data,
    dag=dag,
)

end = DummyOperator(task_id='end', dag=dag)

# Define task dependencies
start >> [scrape_news, scrape_weather, scrape_quotes] >> parse_data >> analyze_data_task >> end 