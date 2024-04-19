# MetaFormer Backend

## Overview
MetaFormer Backend is a powerful Django-based backend that serves as the data processing and API serving layer for handling large datasets with customizable data type inference. It utilizes Django Rest Framework to expose RESTful APIs for various data operations, including file uploads, data downloads, dataset enumeration, and setting preferred data types.

## Features
- Upload and process CSV files with dynamic schema detection.
- Download subsets of data based on user queries.
- Enumerate datasets with configurable depth.
- Manage inference settings and preferred data types on a per-dataset basis.
- RESTful API integration for scalable client-server communication.

## Detailed Process

**Stage 1: Uploading Your Data**

- **Initial Action**: You upload your CSV file through the UI and initiate inference.
- **Behind-the-Scenes**: The server gathers detailed statistics on your data, considering all potential data types for each column. These statistics reflect how well the data fits with each type, including the count of exceptions.

**Stage 2: Presenting Data Types and Statistics**

- **Display of Statistics**: On the UI, full statistics, and up-to-date automated inference results, are presented to you.
- **Exception Tolerance**: This UI feature lets you set a threshold for the number of exceptions allowed by automated inference.

**Stage 3: User Control Over Data Types**

- **User Overrides**: After the UI displays the system's suggestions, you have the option to manually adjust data types where necessary.
- **Integrated Exceptions**: The UI integrates data-type exceptions, internally recorded in a separate JSONB column, directly into the data grid, highlighted in red.

Throughout this process, you're in full control. The server compiles and suggests, but you make the final decisions on how your data is presented and managed.

## Installation

### Prerequisites
- Python 3.8 or higher
- Django 3.1 or higher
- Django Rest Framework
- Memurai 4.1.1 (or Celery)
- Redis server 7.2.4

### Setup

1. **Clone the repository:**
```bash
  git clone https://github.com/yourusername/MetaFormerBackend.git
  cd MetaFormerBackend
```  

2. **Set up a Python virtual environment and activate it:**
```bash
  python -m venv venv
  .\venv\Scripts\activate
```

3. **Install required packages:**
```bash
  pip install -r requirements.txt
```

4. **Migrate the database:**
```bash
  python manage.py migrate
```

5. **Start the development server:**
```bash
  python manage.py runserver
```

6. **Launch Celery to process background tasks:**
```bash
celery -A metaformer  worker --loglevel=debug --concurrency=30 -E -Ofair -P eventlet
```

## API Usage

### Upload Data
`POST /api/upload_data/`
- **Description:** Uploads CSV data along with a schema and initiates processing.
- **Permissions:** Authenticated users only.
- **Data Params:**
- `file`: The CSV file to upload.
- `column_types`: JSON describing column types.
- `schema`: JSON schema for data processing.

### Download Data
`GET /api/download_data/`
- **Description:** Downloads a subset of CSV data based on parameters.
- **Query Params:**
- `dataset_name`: Name of the dataset to download.
- `start_row`: Starting row index.
- `num_rows`: Number of rows to fetch.

### Enumerate Datasets
`GET /api/enumerate_datasets/`
- **Description:** Lists datasets in the system with optional depth specification.
- **Query Params:**
- `path`: Path to enumerate datasets from.
- `depth`: Depth of enumeration.

### Set Preferred Types
`POST /api/preferred_types/`
- **Description:** Sets preferred data types for a dataset.
- **Data Params:**
- `dataset_name`: Name of the dataset.
- `preferred_types`: JSON of preferred types.

### Manage Inference
`POST /api/manage_inference/`
- **Description:** Manages inference settings for a dataset.
- **Data Params:**
- `dataset_name`: Name of the dataset.
- `command`: Command to execute.
- `schema`: JSON schema for passing na_values and max_categories.

## Contributing
Contributions are welcome! Please fork the repository and submit pull requests to the main branch.

## License
This project is licensed under the MIT License - see the LICENSE file for details.