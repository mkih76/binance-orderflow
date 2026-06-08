@echo off
chcp 65001 >nul
cd /d C:\Users\22975\binance-orderflow
python daily_review.py >> daily_reviews\review.log 2>&1
