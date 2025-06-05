# 🔐 Security Notes for Fantasy Football Pipeline

## ⚠️ **CRITICAL SECURITY REMINDER**

**NEVER commit OAuth credentials or API secrets to Git!**

## 📋 **Files That Should NEVER Be Committed:**

### **❌ Sensitive Files (Auto-ignored by .gitignore):**
- `oauth2.json` - Contains actual OAuth tokens
- `config.json` - Contains your Yahoo API credentials  
- Any file containing real API keys, tokens, or secrets

### **✅ Safe Template Files (OK to commit):**
- `data/templates/config.template.json` - Template with placeholders only

## 🔧 **Proper Setup Process:**

1. **Copy template and add YOUR credentials:**
   ```bash
   cp data/templates/config.template.json config.json
   # Edit config.json with your Yahoo API credentials
   ```

2. **Run authentication (creates oauth2.json automatically):**
   ```bash
   python3 src/auth/yahoo_oauth.py
   ```

3. **Verify files are gitignored:**
   ```bash
   git status  # Should NOT show oauth2.json or config.json
   ```

## 🚨 **If You Accidentally Commit Secrets:**

1. **Immediately revoke/regenerate credentials** in Yahoo Developer Console
2. **Remove from git history:**
   ```bash
   git filter-branch --force --index-filter \
   "git rm --cached --ignore-unmatch oauth2.json config.json" \
   --prune-empty --tag-name-filter cat -- --all
   ```
3. **Force push to rewrite history:**
   ```bash
   git push origin --force --all
   ```

## 🛡️ **Security Best Practices:**

- ✅ Always use `.gitignore` patterns for secrets
- ✅ Use environment variables for production deployments  
- ✅ Regularly rotate API credentials
- ✅ Never share credentials in chat/email
- ✅ Use GitHub Secrets for CI/CD pipelines

## 🔍 **Verify Your Security:**

```bash
# Check for accidentally committed secrets
git log --oneline | head -10  # Check recent commits
git ls-files | grep -E "(oauth2|config|secret|token)"  # Should only show templates

# Check current directory for secrets
ls -la | grep -E "(oauth2|config|secret|token)"
```

---

**🏆 Remember: Better safe than sorry when it comes to API credentials!** 