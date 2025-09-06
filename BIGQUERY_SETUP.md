# BigQuery Authentication Setup Guide

## Option 1: Service Account Key (Recommended for Development)

### Step 1: Create Google Cloud Project
1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select existing one
3. Note your Project ID (e.g., "price-pulse-470211")

### Step 2: Enable BigQuery API
1. In Google Cloud Console, go to "APIs & Services" → "Library"
2. Search for "BigQuery API"
3. Click "Enable"

### Step 3: Create Service Account
1. Go to "IAM & Admin" → "Service Accounts"
2. Click "Create Service Account"
3. Fill in details:
   - Name: `bigquery-loader`
   - Description: `Service account for data pipeline BigQuery access`
4. Click "Create and Continue"

### Step 4: Grant Permissions
Add these roles to your service account:
- **BigQuery Data Editor** (read/write data)
- **BigQuery Job User** (run queries)
- **BigQuery User** (access BigQuery service)

### Step 5: Create and Download Key
1. Click on your service account
2. Go to "Keys" tab
3. Click "Add Key" → "Create new key"
4. Choose **JSON** format
5. Download the file (save as `bigquery-service-account.json`)

### Step 6: Update Your Project
1. **Save the key file** in your project directory:
   ```
   D:\Semester 5\DSE Project\data-pipeline\credentials\bigquery-service-account.json
   ```

2. **Update .env file** with correct paths:
   ```properties
   GOOGLE_APPLICATION_CREDENTIALS="D:\Semester 5\DSE Project\data-pipeline\credentials\bigquery-service-account.json"
   GOOGLE_CLOUD_PROJECT="your-actual-project-id"
   ```

3. **Create credentials directory:**
   ```powershell
   mkdir credentials
   ```

4. **Add to .gitignore:**
   ```
   credentials/
   *.json
   ```

## Option 2: Application Default Credentials (Alternative)

If you have Google Cloud SDK installed:

```powershell
# Install Google Cloud SDK first, then run:
gcloud auth application-default login
```

## Option 3: Set Environment Variables Manually

In PowerShell (for current session):
```powershell
$env:GOOGLE_APPLICATION_CREDENTIALS="D:\Semester 5\DSE Project\data-pipeline\credentials\bigquery-service-account.json"
$env:GOOGLE_CLOUD_PROJECT="your-project-id"
```

## Testing Your Setup

After setting up credentials, test with:
```powershell
cd "D:\Semester 5\DSE Project\data-pipeline\transformations\loading\bigquery"
python loader.py
```

## Troubleshooting

### Error: "DefaultCredentialsError"
- Check that `GOOGLE_APPLICATION_CREDENTIALS` points to valid JSON file
- Verify the file exists and is readable
- Make sure the service account has proper permissions

### Error: "Project not found"
- Verify `GOOGLE_CLOUD_PROJECT` matches your actual project ID
- Check that BigQuery API is enabled for your project

### Error: "Permission denied"
- Ensure your service account has BigQuery roles
- Check that the dataset exists or the service account can create it

## Security Best Practices

1. **Never commit credentials to git**
2. **Use least privilege** (only necessary permissions)
3. **Rotate keys regularly**
4. **Use environment-specific service accounts**
