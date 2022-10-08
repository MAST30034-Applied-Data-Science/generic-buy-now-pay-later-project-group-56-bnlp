# Generic Buy Now, Pay Later Project

## Description:

Include the PURPOSE and GOAL.

## Before you run the pipeline:

It is important that you follow the steps outlined below for the pipeline to run correctly:

Store your personal API key in a '.env' file, following the steps below:

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

## Begin:

1. To run the pipeline, simply enter the project on root level and run the following
   `./ETL.sh` 
This will install all relavent python packages and run our ETL python script.

2. To identify Top 'N' Merchants, run 
   `./Rank.sh [N]`
This outputs the top [N] merchants by the 5 categories listed prior, in total
providing [N * 5] merchants for BNPL bussiness to partner with. If [N] is not provided,
then N = 10. i.e `./Rank.sh` will return top 10 Merchants per category; `./Rank.sh 15` will return
top 15 and so on.


## NOTES:

- REMEMBER TO MANUALLY INCLUDE INCOME DATASET FILE.
- Due to formatting issues and the inability to pull the Income Data through any sort of API, it has manually been put (DOUBLE CHECK WITH NOAH).
- REMEBER TO UNCOMMENT ".env" FROM THE .gitignore BEFORE SUBMITTING.

## Authors:

- Shromann Majumder
- Noah Sebastian
- Anuj Bungla
- Bhavleen Kau Sethi
- Derio Luwi