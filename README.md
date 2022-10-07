# Generic Buy Now, Pay Later Project

## Description:

Include the PURPOSE and GOAL.

## Before you run the pipeline:

It is important that you follow the steps outlined below for the pipeline to run correctly:

1. Run 'requirements.txt' to download and install the necessary packages and modules for the python scripts and notebooks to run correctly.

2. Store your personal API key in a '.env' file, following the steps below:

   a) Goto AURIN's main page: https://aurin.org.au/ <br/>
   b) Click on "ACCESS NOW". <br/>
   c) Scroll down until you see the "ACCESS DASHBOARD" heading, then click on "CLICK TO LOGIN" underneath it. <br/>
   d) Click on "Login". <br/>
   e) Search for "The University of Melbourne", then click on "Continue to your organisation". <br/>
   f) Sign into your University of Melbourne account. <br/>
   g) Click on "Approve" and accept the terms and conditions. <br/>
   h) On the LHS, click on the "Data Provider" tab. <br/>
   i) Click on "Generate New Credentials". It will now show your Username and Password (hidden in asterisks). Click on "Show" to show your Password. <br/>
   j) Make a new '.env' file under the 'scripts' directory. <br/>
   k) Write out your username and password from step "i)" in the .env file in the following format and save: <br/>
      username = "copy_and_paste_your_username_here" <br/>
      password = "copy_and_paste_your_password_here"

- REMEMBER TO MANUALLY INCLUDE INCOME DATASET FILE.
- REMEBER TO UNCOMMENT ".env" FROM THE .gitignore BEFORE SUBMITTING.

## Begin:

To run the pipeline, please visit the `scripts` directory and run the files in order:

1. 'etl.py' : This extracts and preprocesses all the raw data, then combines them into a single file 'process_data.parquet'. 

## NOTE:



## Authors:

Shromann Majumder
Noah Sebastian
Anuj Bungla
Bhavleen Kau Sethi
Derio Luwi