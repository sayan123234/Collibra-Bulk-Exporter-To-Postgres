
# Collibra-Bulk-Exporter

This project helps in bulk exporting assets along with their related attributes, relations, and responsibilities by providing `assetTypeId(s)`.The tool stores the data directly into a PostgreSQL database. Asset data is saved in dynamically generated tables based on the `assetTypeName(s)` specified in the configuration.

---

## Setup Instructions

### Setting Up OAuth in Your Collibra Instance

To connect the tool with your Collibra instance, you need to set up OAuth credentials.

1. **Log in to Collibra**:
   - Navigate to your Collibra instance.

2. **Access OAuth Settings**:
   - Go to **Settings** -> **OAuth Applications**.

3. **Register a New Application**:
   - Click on **Register Application**.
   - Set the integration type to **"Integration"** and give the name of the **application**.

4. **Generate Client Credentials**:
   - Copy the `clientId` and `clientSecret`.
   - Add them to the `.env` file as shown below.

---

Follow the steps below to set up and use the Collibra-Bulk-Exporter:

### 1. Clone the Repository

```bash
# Clone the repository from GitHub
$ git clone <repository-url>

# Navigate into the project directory
$ cd Collibra-Bulk-Exporter
```

### 2. Create a Python Virtual Environment

```bash
# Create a virtual environment
$ python -m venv env

# Activate the virtual environment
# On Windows
$ env\Scripts\activate

# On macOS/Linux
$ source env/bin/activate
```

### 3. Install Dependencies

```bash
# Install the required Python packages
$ pip install -r requirements.txt
```

### 4. Set Up the `.env` File

Create a `.env` file in the root directory of the project and add the following environment variables:

```env
# Collibra Connection
COLLIBRA_INSTANCE_URL=your_instance_name.collibra.com
CLIENT_ID=your_client_id
CLIENT_SECRET=your_client_secret

# PostgreSQL Database Configuration
DATABASE_URL=postgresql://db_username:your_strong_password@localhost:5432/your_database_name
```

### 5. Update Asset Type IDs

Edit the `Collibra_Asset_Type_Id_Manager.json` file in the root directory to include the asset type IDs you want to export. For example:

```json
{
    "assetTypeIds": [
        "asset_type_id_1",
        "asset_type_id_2"
    ]
}
```

### 6. Run the Application

Run the script to save the assets to the PostgreSQL database:

```bash
$ python main.py
```

---

## Database Integration Details

- **Table Creation**: Dynamically creates tables for each asset type.
- **Primary Key**: The `uuid` column from the Collibra asset is used as the primary key.
- **Timestamp**: The `last_modified_on` column tracks the last modification timestamp.
- **Data Storage**: All additional attributes are stored as string columns.
- **Upsert Strategy**: Updates existing records if they already exist, otherwise inserts new records.

---

## Troubleshooting

### Database Connection Issues:

- Verify that PostgreSQL is running.
- Check the `DATABASE_URL` in the `.env` file.
- Ensure the database exists and the user has the correct permissions.

### Collibra Connection Problems:

- Verify the `COLLIBRA_INSTANCE_URL`.
- Check OAuth credentials (`CLIENT_ID` and `CLIENT_SECRET`).
- Ensure the application has the required permissions.

### Performance Considerations:

- Large datasets may take considerable time to process.
- Monitor system resources during the operation.
- Consider adding batch processing configurations if necessary.

---

## Security Notes

- Keep the `.env` file secure and out of version control.
- Use strong, unique passwords for database access.
- Limit database user permissions to only what is necessary.
- Consider using environment-specific configurations.

---

## Future Enhancements

- Support for multiple database backends.
- Configurable batch processing for better performance.
- Enhanced logging and monitoring features.
- Advanced data transformation and enrichment capabilities.

Enjoy using the Collibra-Bulk-Exporter!
