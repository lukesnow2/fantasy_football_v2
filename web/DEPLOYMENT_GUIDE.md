# 🚀 Session Management & Deployment Guide

## 🔧 Fixed Session Management Issues

### Problems Solved:
1. **Database Schema Mismatch**: Session table now properly in `app` schema
2. **Cookie Security**: Added httpOnly, secure, and sameSite settings
3. **Connection Configuration**: Updated to support both `edw` and `app` schemas
4. **Session Validation**: Added proper debugging and error handling

### Changes Made:
- ✅ Updated `src/lib/server/db/index.ts` for multi-schema support
- ✅ Enhanced `src/lib/server/auth.ts` with secure cookie settings
- ✅ Added migration functionality to debug API endpoint
- ✅ Created Vercel deployment configuration

## 🧪 Testing Session Management

### 1. Run Database Migration
Visit your local development server and go to:
```
http://localhost:5173/api/debug?action=migrate
```

This will:
- Ensure `app` schema exists
- Move session/user tables from `public` to `app` schema
- Create proper table structures

### 2. Test Database Schema
```
http://localhost:5173/api/debug?action=test-schemas
```

This will show:
- Available schemas
- Tables in app schema
- Current session/user counts

### 3. Test Authentication Flow
1. Go to `/login`
2. Register a new account or login
3. Verify you stay logged in after page refresh
4. Check session persistence across browser tabs

## 🌐 Vercel Deployment Setup

### 1. Environment Variables
Set these in your Vercel dashboard (Settings → Environment Variables):

```bash
# Database (REQUIRED)
DATABASE_URL=your-heroku-postgres-url

# Authentication Secrets (REQUIRED)
AUTH_SECRET=your-32-character-secret
SESSION_SECRET=your-32-character-secret

# Application Config
NODE_ENV=production
ORIGIN=https://your-domain.vercel.app

# Email (Optional)
EMAIL_PROVIDER=sendgrid
SENDGRID_API_KEY=your-sendgrid-key
EMAIL_FROM=noreply@your-domain.com
```

### 2. Generate Secrets
```bash
# Generate AUTH_SECRET
openssl rand -base64 32

# Generate SESSION_SECRET  
openssl rand -base64 32
```

### 3. Deploy to Vercel
```bash
# Install Vercel CLI
npm i -g vercel

# Deploy
cd web
vercel --prod
```

## 🔒 Security Features

### Production Cookies
- `httpOnly: true` - Prevents XSS attacks
- `secure: true` - HTTPS only in production
- `sameSite: 'lax'` - CSRF protection

### Database Security
- SSL connections required
- Schema-based data isolation
- Indexed for performance

### Headers
- Content Security Policy headers
- XSS protection
- Frame-Options for clickjacking protection

## 📊 Monitoring & Debugging

### Debug Endpoints (Development Only)
- `/api/debug` - System status
- `/api/debug?action=migrate` - Run migration
- `/api/debug?action=test-schemas` - Test database
- `/api/debug/session` - Session debugging
- `/api/debug/sessions` - List active sessions

### Common Issues & Solutions

#### 1. "No session found in database"
**Cause**: Session table in wrong schema
**Solution**: Run migration via `/api/debug?action=migrate`

#### 2. "Form actions expect form-encoded data"
**Cause**: Missing `action` attribute on forms
**Solution**: Ensure forms have proper `action="?/login"` attributes

#### 3. Cookies being deleted
**Cause**: Insecure cookie settings or domain mismatch
**Solution**: Check ORIGIN environment variable matches deployment URL

#### 4. Database connection errors
**Cause**: Missing SSL or wrong connection string
**Solution**: Ensure DATABASE_URL includes `?sslmode=require`

## 🚀 Post-Deployment Checklist

- [ ] Environment variables set in Vercel
- [ ] Database migration completed
- [ ] Authentication flow tested
- [ ] Session persistence verified
- [ ] Password reset emails working (if configured)
- [ ] Debug endpoints disabled in production
- [ ] HTTPS redirect working
- [ ] Custom domain configured (if applicable)

## 📈 Performance Optimization

### Database Indexes
- `idx_user_manager_key` - User manager lookups
- `idx_user_email` - Email-based queries
- `idx_session_expires` - Session cleanup

### Connection Pooling
- Max 1 connection per serverless function
- SSL required for Heroku Postgres
- Connection reuse optimized

## 🔄 Maintenance

### Session Cleanup
Sessions automatically expire after 30 days. For manual cleanup:
```sql
DELETE FROM app.session WHERE expires_at < NOW();
```

### User Management
Placeholder users can be claimed during registration. To list unclaimed accounts:
```sql
SELECT * FROM app.user WHERE account_status = 'placeholder';
```

## 🎯 Next Steps

1. **Test thoroughly** in development
2. **Deploy to Vercel** with proper environment variables
3. **Set up monitoring** (optional: Sentry integration)
4. **Configure custom domain** if needed
5. **Set up email provider** for production features

---

## 📞 Troubleshooting

If you encounter issues:
1. Check Vercel function logs
2. Use debug endpoints in development
3. Verify environment variables
4. Test database connectivity
5. Check browser network/console for errors

**Remember**: Debug endpoints are automatically disabled in production for security. 