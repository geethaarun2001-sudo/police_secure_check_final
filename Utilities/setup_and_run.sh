#!/bin/bash

echo "============================================================"
echo "  Police Digital Ledger - Complete Setup"
echo "============================================================"
echo ""

# Step 1: Install dependencies
echo "Step 1: Installing Python dependencies..."
echo "-----------------------------------------------------------"
pip3 install mysql-connector-python pandas streamlit

if [ $? -ne 0 ]; then
    echo "❌ Failed to install dependencies"
    echo "Try running manually: pip3 install mysql-connector-python pandas streamlit"
    exit 1
fi

echo ""
echo "✅ Dependencies installed successfully!"
echo ""

# Step 2: Verify installation
echo "Step 2: Verifying installation..."
echo "-----------------------------------------------------------"
python3 -c "import mysql.connector; import pandas; import streamlit; print('✅ All packages imported successfully!')"

if [ $? -ne 0 ]; then
    echo "❌ Package verification failed"
    exit 1
fi

echo ""

# Step 3: Test database connection
echo "Step 3: Testing database connection..."
echo "-----------------------------------------------------------"
python3 test_db_connection.py

if [ $? -ne 0 ]; then
    echo "❌ Database connection test failed"
    echo "Please check your database credentials and network connection"
    exit 1
fi

echo ""

# Step 4: Ask user if they want to create the table
echo "Step 4: Database setup"
echo "-----------------------------------------------------------"
read -p "Do you want to create the table and ingest data? (y/n) " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Creating table and ingesting data..."
    echo "This will take 2-5 minutes. Please wait..."
    echo ""
    python3 2nd_step_db_schema_connection_setup.py
    
    if [ $? -ne 0 ]; then
        echo "❌ Database setup failed"
        exit 1
    fi
    
    echo ""
    echo "✅ Database setup completed!"
    echo ""
    
    # Verify table creation
    echo "Verifying table creation..."
    python3 test_db_connection.py
fi

echo ""
echo "============================================================"
echo "  Setup Complete!"
echo "============================================================"
echo ""
echo "To run the dashboard, execute:"
echo "  streamlit run 3rd_step_streamlit_dashboard.py"
echo ""
echo "Or run this script with the --run-dashboard flag:"
echo "  bash setup_and_run.sh --run-dashboard"
echo ""

# Check if --run-dashboard flag is provided
if [[ "$1" == "--run-dashboard" ]]; then
    echo "Starting dashboard..."
    streamlit run 3rd_step_streamlit_dashboard.py
fi

