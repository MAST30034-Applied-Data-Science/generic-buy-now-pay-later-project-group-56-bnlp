# Generic Buy Now, Pay Later Project

## Description:

In a nutshell, the Buy Now Pay Later firm offers its services to their partenered merchants, which allows their customers to pay for items in five installments, instead of all upfront. The firm itself gets a small commission for every transaction the customer makes with the buy now pay later feature. However, due to limited resources, the firm is only able to onboard a handful of merchants to partner up with every year.

Hence, the objective and purpose of this project is to rank those merchants based on how well they perform and how consistent they are with their sales, such that the profits for the firm are optimized. In addition to this, a summary notebook is also included which summarises the overall approach taken, issues and obstacles that the team ran into, limitations/assumptions that were made, as well as recommendations to the client and stakeholders.

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
   j) Make a new '.env' file under the `scripts` directory. <br/>
   k) Write out your username and password from step "i)" in the .env file in the following format and save: <br/>
      username = "copy_and_paste_your_username_here" <br/>
      password = "copy_and_paste_your_password_here"

## Begin:

1. To run the pipeline, simply enter the project on root level and run the following
   `./ETL.sh` 
This will install all relevant python packages and run our ETL python script. The file assumes that the datasets are located in the data/tables folder. If they are not, change the path in `./ETL.sh` after "--path" to the correct location of the datasets.

2. To identify Top 'N' Merchants, run 
   `./Rank.sh [N]`
This outputs the top [N] merchants by the 5 categories listed prior, in total
providing [N * 5] merchants for BNPL bussiness to partner with. If [N] is not provided,
then N = 10. i.e `./Rank.sh` will return top 10 Merchants per category; `./Rank.sh 15` will return
top 15 and so on.

3. The notebook `summary.ipynb` in the notebooks folder gives a summary of the overall approach of the project as well as recommendations.

## Notes:

- Due to formatting issues and the inability to pull the Income Data through any sort of API, 
it has been left under the external datasets as `income_data_raw.csv`.

## Authors:

- Shromann Majumder
- Noah Sebastian
- Anuj Bungla
- Bhavleen Kau Sethi
- Derio Luwi
