# Google Sheets Setup Guide

This guide explains how to set up Google Sheets integration for the Domo-to-Snowflake project.

## Prerequisites

1. **Google Account**: You need a Google account with access to Google Sheets
2. **Google Cloud Project**: A project in Google Cloud Console
3. **Python Environment**: Python 3.7+ with pip

## Step 1: Install Dependencies

Install the required Google Sheets libraries:

```bash
pip install -r requirements-googlesheets.txt
```

## Step 2: Google Cloud Console Setup

### 2.1 Create/Select Project

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select an existing one
3. Note your **Project ID**

### 2.2 Enable Google Sheets API

1. In the Google Cloud Console, go to **APIs & Services** > **Library**
2. Search for "Google Sheets API"
3. Click on it and press **Enable**

### 2.3 Create Service Account (Recommended)

1. Go to **APIs & Services** > **Credentials**
2. Click **Create Credentials** > **Service Account**
3. Fill in the service account details:
   - **Name**: `domo-sheets-integration`
   - **Description**: `Service account for Domo to Google Sheets integration`
4. Click **Create and Continue**
5. Skip the optional steps and click **Done**

### 2.4 Generate Service Account Key

1. Click on your newly created service account
2. Go to the **Keys** tab
3. Click **Add Key** > **Create New Key**
4. Choose **JSON** format
5. Download the JSON file and save it securely
6. **Important**: Keep this file secure and never commit it to version control

### 2.5 Grant Permissions

1. Copy the **Service Account Email** from the service account details
2. Share your Google Sheets with this email address
3. Give it **Editor** permissions

## Step 3: Environment Configuration

Create a `.env` file in your project root with the following variables:

```bash
# Google Sheets Configuration
GOOGLE_SHEETS_CREDENTIALS_FILE=/path/to/your/service-account-key.json
GOOGLE_SHEETS_SPREADSHEET_ID=your-spreadsheet-id-here

# Existing Domo Configuration
DOMO_DEVELOPER_TOKEN=your-domo-token
DOMO_INSTANCE=your-domo-instance

# Existing Snowflake Configuration
SNOWFLAKE_USER=your-snowflake-user
SNOWFLAKE_PASSWORD=your-snowflake-password
SNOWFLAKE_ACCOUNT=your-snowflake-account
SNOWFLAKE_WAREHOUSE=your-warehouse
SNOWFLAKE_DATABASE=your-database
SNOWFLAKE_SCHEMA=your-schema
```

### Finding Your Spreadsheet ID

1. Open your Google Sheets in a web browser
2. Look at the URL: `https://docs.google.com/spreadsheets/d/SPREADSHEET_ID/edit`
3. Copy the `SPREADSHEET_ID` part

## Step 4: Test the Setup

Run the example script to test your configuration:

```bash
python google_sheets_example.py
```

## Authentication Methods

### Method 1: Service Account (Recommended)

- **Pros**: No user interaction required, works in automated environments
- **Cons**: Requires sharing spreadsheets with service account email
- **Setup**: Use `GOOGLE_SHEETS_CREDENTIALS_FILE` environment variable

### Method 2: OAuth2 (User Authentication)

- **Pros**: Uses your personal Google account, no need to share spreadsheets
- **Cons**: Requires user interaction for authentication
- **Setup**: Use `GOOGLE_CLIENT_ID` and `GOOGLE_CLIENT_SECRET` environment variables

## Usage Examples

### Reading from Google Sheets

```python
from utils.googlesheets import GoogleSheetsHandler

sheets_handler = GoogleSheetsHandler()
sheets_handler.setup_auth()
sheets_handler.open_spreadsheet("your-spreadsheet-id")
df = sheets_handler.read_worksheet("Sheet1")
```

### Writing to Google Sheets

```python
from utils.googlesheets import GoogleSheetsHandler

sheets_handler = GoogleSheetsHandler()
sheets_handler.setup_auth()
sheets_handler.open_spreadsheet("your-spreadsheet-id")
success = sheets_handler.write_worksheet(df, "Output_Sheet")
```

### Domo to Google Sheets

```python
from utils.domo import DomoHandler
from utils.googlesheets import GoogleSheetsHandler

# Extract from Domo
domo_handler = DomoHandler()
domo_handler.setup_auth()
df = domo_handler.extract_data("your-dataset-id")

# Write to Google Sheets
sheets_handler = GoogleSheetsHandler()
sheets_handler.setup_auth()
sheets_handler.open_spreadsheet("your-spreadsheet-id")
sheets_handler.write_worksheet(df, "Domo_Data")
```

## Troubleshooting

### Common Issues

1. **"Service account not found"**
   - Make sure you've shared the spreadsheet with the service account email
   - Check that the service account has Editor permissions

2. **"Invalid credentials"**
   - Verify the path to your service account JSON file
   - Check that the JSON file is valid and not corrupted

3. **"API not enabled"**
   - Make sure Google Sheets API is enabled in your Google Cloud project

4. **"Permission denied"**
   - Check that your service account has the necessary scopes
   - Verify the spreadsheet ID is correct

### Debug Mode

Enable debug logging to see more detailed error messages:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Security Best Practices

1. **Never commit credentials to version control**
2. **Use environment variables for sensitive data**
3. **Rotate service account keys regularly**
4. **Limit service account permissions to minimum required**
5. **Monitor API usage in Google Cloud Console**

## API Limits

- **Google Sheets API**: 300 requests per minute per user
- **Service accounts**: 300 requests per minute per project
- **Large datasets**: Consider chunking data for better performance

## Cost Considerations

- **Google Sheets API**: Free tier includes 300 requests per minute
- **Additional requests**: $0.01 per 1,000 requests
- **Service accounts**: Free to create and use 