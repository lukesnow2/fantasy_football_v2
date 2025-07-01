# 🔐 Security Guidelines

## ⚠️ Never Commit Credentials

**Critical**: Never commit OAuth tokens or API secrets to Git!

## 🔧 Secure Setup

### Safe Files to Commit ✅
- `data/templates/config.template.json` - Template with placeholders only
- Any file with `.template.` in the name

### Never Commit ❌
- `config.json` - Contains your Yahoo API credentials
- `oauth2.json` - Contains OAuth tokens  
- Any file with real API keys or secrets

## 📋 Proper Setup Process

```bash
# 1. Copy template (safe)
cp data/templates/config.template.json config.json

# 2. Edit with your credentials (now protected by .gitignore)
# Edit config.json with your Yahoo API credentials

# 3. Run authentication (creates oauth2.json automatically)
python3 src/auth/yahoo_oauth.py

# 4. Verify files are ignored
git status  # Should NOT show config.json or oauth2.json
```

## 🛡️ Production Security

### GitHub Actions
Use repository secrets (Settings → Secrets → Actions):
```
YAHOO_CLIENT_ID=your_client_id
YAHOO_CLIENT_SECRET=your_client_secret
YAHOO_REFRESH_TOKEN=your_refresh_token
HEROKU_DATABASE_URL=your_database_url
```

### Environment Variables
```bash
# Set sensitive values as environment variables
export DATABASE_URL="your-postgres-connection-string"
export YAHOO_CLIENT_ID="your-client-id"
```

## 🚨 If You Accidentally Commit Secrets

### Immediate Actions
1. **Revoke credentials** in Yahoo Developer Console
2. **Generate new API keys**
3. **Update GitHub secrets** with new credentials

### Clean Git History
```bash
# Remove from all git history (nuclear option)
git filter-branch --force --index-filter \
  "git rm --cached --ignore-unmatch config.json oauth2.json" \
  --prune-empty --tag-name-filter cat -- --all

git push origin --force --all
```

## ✅ Security Checklist

- [ ] Used template files for setup
- [ ] Never committed real credentials
- [ ] Used GitHub secrets for automation  
- [ ] Set proper environment variables
- [ ] Rotated credentials annually
- [ ] Enabled two-factor authentication

## 🔍 Verification Commands

```bash
# Check no sensitive files are tracked
git ls-files | grep -E "(oauth2|config)" | grep -v template
# Should return nothing

# Check git status is clean
git status
# Should not show oauth2.json or config.json
```

---

**Security Status: Repository is clean and protected** ✅ 