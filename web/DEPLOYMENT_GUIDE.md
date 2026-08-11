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
- ✅ Created Vercel deployment configuration


## Why `installCommand` is `npm install --include=dev`

`vercel.json` pins the install command with `--include=dev`, and that flag is
load-bearing. This project sets `NODE_ENV=production` as a Vercel environment
variable, which makes `npm install` omit devDependencies. `vite` and
`svelte-kit` are devDependencies, so the build runs with no build tool and dies
with `vite: command not found` (exit 127) after installing only ~114 packages.

The flag makes the install correct however `NODE_ENV` is set. Removing the
manually-set `NODE_ENV=production` variable is also worth doing — Vercel sets it
in the function runtime on its own — but the flag is the durable fix.

Note that `vercel.json` is validated against a strict schema and rejects unknown
top-level keys, so this explanation lives here rather than as a comment in the
file. A stray `_note` key fails the deployment before the build even starts,
with only a link to the project-configuration docs to go on.

## 🧪 Testing Session Management

### 1. Run Database Migration
Migrations are driven by drizzle-kit, not by an HTTP endpoint:
```bash
npm run db:generate   # author a migration from schema.ts changes
npm run db:migrate    # apply pending migrations
```
`npm run db:push` is for local development only — never run it against production.

### 2. Test Database Schema
Inspect the database directly with `npm run db:studio`, or with `psql "$DATABASE_URL"`.

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

### Debug Endpoints

Removed. `/api/debug`, `/api/debug/session`, `/api/debug/sessions`, and `/api/db-test` were
unauthenticated in production: `/api/debug/session` returned the caller's raw `auth-session`
token, `/api/debug/sessions` dumped session rows, and `/api/debug?action=migrate` executed DDL.

Use `npm run db:studio`, `psql "$DATABASE_URL"`, and `vercel logs` instead.

### Common Issues & Solutions

#### 1. "No session found in database"
**Cause**: Session table in wrong schema
**Solution**: Confirm `search_path` includes `app`, then run `npm run db:migrate`

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