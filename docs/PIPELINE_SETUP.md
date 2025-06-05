# Fantasy Football Data Pipeline Setup Guide

## 🏆 Overview

This guide helps you set up **fully automated** weekly data extraction for your fantasy football leagues. The pipeline runs every Sunday during the season (August 18th - January 18th) with **zero maintenance required**.

## ✅ **Current Status: PRODUCTION READY**

The pipeline is currently **live and operational** with:
- ✅ **GitHub Actions workflow** deployed and tested
- ✅ **Email notifications** configured via GitHub  
- ✅ **Heroku PostgreSQL database** with complete schema + draft data
- ✅ **20+ years historical data** fully processed and deployed
- ✅ **Smart season detection** - automatically skips off-season

## 🎯 Orchestration Options

### ✅ **Option 1: GitHub Actions (Recommended)**

**Pros:**
- ✅ Free for public repositories
- ✅ Integrated with your codebase
- ✅ Reliable with good error handling
- ✅ Easy seasonal scheduling
- ✅ Automatic artifact storage

**Setup Steps:**

#### 1. Set up GitHub Secrets
In your GitHub repository, go to **Settings > Secrets and Variables > Actions** and add:

```
YAHOO_CLIENT_ID: your_yahoo_app_client_id
YAHOO_CLIENT_SECRET: your_yahoo_app_client_secret  
YAHOO_REFRESH_TOKEN: your_long_lived_refresh_token
HEROKU_DATABASE_URL: your_heroku_postgres_url
```

#### 2. Push the Workflow
The workflow file `.github/workflows/weekly-data-extraction.yml` is already created. Just commit and push it.

#### 3. Test the Pipeline
- Go to **Actions** tab in GitHub
- Click **Weekly Fantasy Football Data Pipeline**
- Click **Run workflow** > **Run workflow**

---

### 🔄 **Option 2: Heroku Scheduler**

**Pros:**
- ✅ Runs on same infrastructure as your database
- ✅ Simple setup
- ✅ Good for small jobs

**Cons:**
- ❌ Limited to 10 min execution (may timeout)
- ❌ Less flexibility for seasonal logic

**Setup:**
```bash
# Add Heroku Scheduler addon
heroku addons:create scheduler:standard --app luke-fantasy-football-app

# Add weekly job
heroku addons:open scheduler --app luke-fantasy-football-app
```

Then add job: `python3 weekly_data_extraction.py` to run weekly on Sundays.

---

### 🖥️ **Option 3: Local Cron Job**

**For running on your own server/computer:**

```bash
# Edit crontab
crontab -e

# Add this line for Sunday 6 AM during fantasy season
0 6 * 8-12,1 0 cd /path/to/the-league && python3 weekly_data_extraction.py
```

---

### ☁️ **Option 4: Cloud Schedulers**

**AWS CloudWatch Events:**
```yaml
ScheduleExpression: "cron(0 14 ? 8-12,1 SUN *)"  # 6 AM PST Sundays
```

**Google Cloud Scheduler:**
```bash
gcloud scheduler jobs create http weekly-fantasy-extract \
  --schedule="0 14 * 8-12,1 0" \
  --time-zone="America/Los_Angeles" \
  --uri="https://your-app.herokuapp.com/extract"
```

---

## 📅 Schedule Details

### **When it runs:**
- **Every Sunday at 6 AM PST**
- **Only during fantasy season: August 18th - January 18th**
- **Automatically detects off-season and skips**

### **Why Sunday 6 AM?**
- ✅ All weekend games are complete
- ✅ Yahoo has processed final stats
- ✅ Before most people check their lineups
- ✅ Captures Saturday night/Sunday scoring

---

## 🔧 Pipeline Configuration

### **Weekly vs Full Extraction**

#### 📊 **Weekly Extraction** (Default)
- **Runs:** Every Sunday automatically
- **Focuses on:** Current season data only
- **Extracts:** Recent rosters, matchups, transactions, updated standings
- **Runtime:** ~2-5 minutes
- **Use for:** Regular season updates

#### 🔄 **Full Extraction** (Manual)
- **Runs:** Manually triggered or start of season
- **Focuses on:** All historical data + draft data
- **Extracts:** Everything including 20-year history
- **Runtime:** ~10-15 minutes  
- **Use for:** Season start, major updates, backfills

### **Triggering Full Extraction**

**GitHub Actions:**
1. Go to Actions tab
2. Select "Weekly Fantasy Football Data Pipeline" 
3. Click "Run workflow"
4. Set "Force full data extraction" to `true`

---

## 🛠️ Customization Options

### **Change Schedule**
Edit the cron expression in the workflow:
```yaml
# Current: Every Sunday 6 AM PST during season
- cron: '0 14 * 8-12,1 0'

# Every Tuesday 8 PM PST during season  
- cron: '0 4 * 8-12,1 2'

# Twice weekly: Sunday 6 AM + Wednesday 8 PM
- cron: '0 14 * 8-12,1 0'  # Sunday
- cron: '0 4 * 8-12,1 3'   # Wednesday
```

### **Change Season Dates**
Modify the date check in the workflow or weekly script:
```bash
# Current: Aug 18 - Jan 18
season_start="${current_year}-08-18"  
season_end="$((current_year + 1))-01-18"

# Custom: Sep 1 - Feb 1
season_start="${current_year}-09-01"
season_end="$((current_year + 1))-02-01"
```

### **Add Notifications**

**Slack Integration:**
```yaml
- name: Notify Slack
  uses: 8398a7/action-slack@v3
  with:
    status: ${{ job.status }}
    channel: '#fantasy-football'
    webhook_url: ${{ secrets.SLACK_WEBHOOK }}
```

**Email Notifications:**
```yaml
- name: Send Email
  uses: dawidd6/action-send-mail@v3
  with:
    server_address: smtp.gmail.com
    server_port: 465
    username: ${{ secrets.EMAIL_USERNAME }}
    password: ${{ secrets.EMAIL_PASSWORD }}
    subject: Fantasy Football Data Pipeline Status
    body: Pipeline completed successfully!
    to: your-email@gmail.com
```

---

## 🔍 Monitoring & Troubleshooting

### **Check Pipeline Status**
- **GitHub Actions:** Actions tab shows run history and logs
- **Heroku:** `heroku logs --tail --app luke-fantasy-football-app`

### **Common Issues**

#### **OAuth Token Expired**
```bash
# Update refresh token in GitHub secrets
# Tokens typically last 1 year
```

#### **Rate Limiting**
```bash
# Reduce frequency or add delays between API calls
time.sleep(0.5)  # Increase delay
```

#### **Database Connection Issues**
```bash
# Check Heroku DATABASE_URL is current
heroku config:get DATABASE_URL --app luke-fantasy-football-app
```

### **Manual Recovery**
If a weekly run fails, you can manually trigger:
```bash
# Run locally
python3 weekly_data_extraction.py

# Deploy manually  
python3 deploy_to_heroku_postgres.py --data-file yahoo_fantasy_weekly_data_*.json
```

---

## 📊 Expected Data Volume

### **Weekly Updates**
- **Current season leagues:** 1-2 leagues
- **Teams:** 10-20 teams
- **Records per week:** ~50-200 records
- **File size:** ~100KB - 1MB
- **Runtime:** 2-5 minutes

### **Full Extraction**
- **All leagues:** 26 leagues across 20 years
- **Total records:** ~16,000 records
- **File size:** ~5MB
- **Runtime:** 10-15 minutes

---

## 🚀 Deployment Checklist

- [ ] GitHub secrets configured
- [ ] Workflow file committed and pushed
- [ ] Test run successful
- [ ] Database connection verified
- [ ] Heroku app has sufficient dyno hours
- [ ] Monitor first few automated runs
- [ ] Set up notifications (optional)
- [ ] Document any customizations

---

## 🔗 Useful Commands

```bash
# Test OAuth locally
python3 yahoo_fantasy_oauth_v2.py

# Run weekly extraction locally  
python3 weekly_data_extraction.py

# Check Heroku database
heroku pg:info --app luke-fantasy-football-app

# View GitHub Actions logs
# Go to repository > Actions > Select run > View logs

# Manual deployment
python3 deploy_to_heroku_postgres.py
```

---

## 💡 Best Practices

1. **Test first** - Run manual extractions before relying on automation
2. **Monitor closely** - Watch the first few automated runs
3. **Keep backups** - GitHub automatically stores artifacts for 30 days
4. **Update tokens** - Yahoo refresh tokens expire annually
5. **Check logs** - Monitor for API changes or issues
6. **Seasonal prep** - Test before each new season starts
7. **Resource monitoring** - Ensure Heroku dyno hours are sufficient

Your fantasy football data pipeline is now ready for automated operation! 🏆 