# Rule Proposal and Voting System

## Overview

The Rule Proposal and Voting System allows league members to propose, discuss, and vote on rule changes for the upcoming season. This system ensures democratic governance of the league constitution.

## Features

### 1. Proposal Creation
- **Add New Clause**: Propose entirely new rules or sections
- **Edit Current Language**: Modify existing rule text
- **Tweak Existing Rule**: Make minor adjustments to current rules

### 2. Voting System
- **Vote Types**: Yes, No, or Abstain
- **Comments**: Optional feedback on proposals
- **Real-time Results**: Live vote counting and display
- **Required Majority**: Configurable voting thresholds (default: 7 votes for super-majority)

### 3. Proposal Lifecycle
1. **Draft**: Initial proposal creation
2. **Active**: Open for voting
3. **Passed**: Approved by league vote
4. **Rejected**: Failed to meet voting threshold
5. **Archived**: Historical record

### 4. Amendment Tracking
- Automatic creation of amendment records for passed proposals
- Integration with constitution amendment history
- Effective season tracking

## Database Schema

### Tables

#### `edw.rule_proposal`
- Stores all rule change proposals
- Tracks proposal status, voting periods, and results
- Links to submitting manager

#### `edw.rule_vote`
- Individual votes cast by league members
- One vote per manager per proposal
- Includes optional comments

#### `edw.rule_amendment`
- Historical record of passed amendments
- Links to original proposal
- Tracks approval details

## API Endpoints

### Rule Proposals
- `GET /api/rule-proposals` - List all proposals
- `POST /api/rule-proposals` - Create new proposal
- `PUT /api/rule-proposals` - Update proposal

### Rule Votes
- `GET /api/rule-votes` - Get votes for proposals
- `POST /api/rule-votes` - Submit or update vote

### Managers
- `GET /api/managers` - List league managers

## Usage

### Creating a Proposal

1. Navigate to "Rule Proposals" in the main navigation
2. Click "Create New Proposal"
3. Fill in the proposal details:
   - **Title**: Brief, descriptive name
   - **Type**: Add clause, edit language, or tweak rule
   - **Description**: Overview of the change
   - **Rationale**: Why this change is needed
   - **Effective Season**: When the change takes effect
   - **Current Language**: Existing rule text (for edits)
   - **Proposed Language**: New rule text

4. Submit the proposal

### Voting on Proposals

1. View active proposals in the list
2. Expand a proposal to see details
3. Cast your vote (Yes/No/Abstain)
4. Add optional comment
5. Submit vote

### Managing Proposals

- **Commissioner Actions**:
  - Activate proposals for voting
  - Set voting periods
  - Close voting and determine results
  - Archive completed proposals

## Implementation Notes

### Database Migration

Run the migration script to create the required tables:

```bash
cd web
npm install
node scripts/deploy_rule_proposals.js
```

### Environment Variables

Ensure `DATABASE_URL` is set in your environment.

### Authentication

Currently uses hardcoded manager keys. Future implementation should integrate with user authentication system.

## Future Enhancements

1. **Email Notifications**: Alert league members of new proposals
2. **Discussion Threads**: Allow comments and discussion on proposals
3. **Proposal Templates**: Pre-built templates for common rule changes
4. **Voting Deadlines**: Automatic proposal expiration
5. **Amendment Integration**: Automatic constitution updates for passed proposals
6. **Mobile Support**: Responsive design for mobile voting

## Technical Details

### Dependencies
- `nanoid`: Generate unique proposal IDs
- `pg`: PostgreSQL client for migrations
- `drizzle-orm`: Database ORM

### File Structure
```
web/src/routes/
├── rule-proposals/
│   └── +page.svelte          # Main proposals page
├── api/
│   ├── rule-proposals/
│   │   └── +server.ts        # Proposals API
│   ├── rule-votes/
│   │   └── +server.ts        # Votes API
│   └── managers/
│       └── +server.ts        # Managers API
└── lib/server/db/
    ├── schema.ts             # Database schema
    └── migrations/
        └── add_rule_proposals.sql
```

## Security Considerations

1. **Vote Validation**: Ensure one vote per manager per proposal
2. **Proposal Ownership**: Only allow proposal creators to edit drafts
3. **Voting Periods**: Prevent voting outside designated periods
4. **Data Integrity**: Foreign key constraints and unique indexes
5. **Audit Trail**: Track all proposal and vote changes 