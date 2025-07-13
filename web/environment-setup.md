# Environment Configuration Guide

This guide shows you how to set up environment variables for production deployment.

## Required Environment Variables

Create a `.env` file in your `web` directory with these variables:

```bash
# Database Configuration (REQUIRED)
DATABASE_URL=postgresql://username:password@localhost:5432/fantasy_league_db

# Authentication Secrets (REQUIRED) 
# Generate these with: openssl rand -base64 32
AUTH_SECRET=your-super-secret-auth-key-here-minimum-32-characters
SESSION_SECRET=your-session-secret-key-here-minimum-32-characters

# Application Configuration
NODE_ENV=production
ORIGIN=https://yourleague.com
PORT=3000
```

## Email Configuration (Recommended)

For password reset functionality:

```bash
# SendGrid (Recommended)
EMAIL_PROVIDER=sendgrid
SENDGRID_API_KEY=your-sendgrid-api-key
EMAIL_FROM=noreply@yourleague.com
EMAIL_FROM_NAME="Fantasy League"

# Alternative: Console mode (emails logged to console)
EMAIL_PROVIDER=console
```

## Optional Configuration

```bash
# Security
BCRYPT_ROUNDS=12
SESSION_LIFETIME_HOURS=24

# Rate Limiting
RATE_LIMIT_REQUESTS_PER_MINUTE=60
RATE_LIMIT_WINDOW_MS=60000

# Monitoring (Optional)
SENTRY_DSN=your-sentry-dsn-for-error-tracking
ENABLE_ANALYTICS=false
```

## Generating Secrets

Generate secure secrets for production:

```bash
# Generate AUTH_SECRET
openssl rand -base64 32

# Generate SESSION_SECRET  
openssl rand -base64 32
```

## Production Deployment Checklist

- [ ] Set `NODE_ENV=production`
- [ ] Use HTTPS (`ORIGIN=https://yourdomain.com`)
- [ ] Set secure `DATABASE_URL` for production database
- [ ] Configure email provider (SendGrid recommended)
- [ ] Generate strong `AUTH_SECRET` and `SESSION_SECRET`
- [ ] Set appropriate `SESSION_LIFETIME_HOURS` (24 is recommended)
- [ ] Configure rate limiting as needed
- [ ] Set up error monitoring (Sentry)

## Email Provider Setup

### SendGrid Setup
1. Create SendGrid account
2. Get API key from SendGrid dashboard
3. Set `SENDGRID_API_KEY` in environment
4. Set `EMAIL_FROM` to your verified sender email

### Console Mode (Development)
- Set `EMAIL_PROVIDER=console`
- Password reset emails will be logged to console instead of sent

## Security Notes

- Never commit `.env` files to version control
- Use strong, unique secrets for each environment
- Rotate secrets regularly in production
- Use HTTPS in production
- Set secure cookie flags (handled automatically) 