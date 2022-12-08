# install all dependencies for the project
echo '================== Installing Required Python Packages =================='
pip3 install -r requirements.txt
# run the etl script
echo '======================== Running ETL Script ========================'
python3 ./scripts/etl_with_argparse.py --path ./data/tables/ --output ./data/curated/ --models ./models --train_model 1