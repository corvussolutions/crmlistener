#!/usr/bin/env python3
"""
ActiveCampaign Webhook Receiver - Production Edition

Receives and processes ActiveCampaign webhook events for profile field updates.
Designed for deployment on Render.com with Github auto-deploy.

Deployment Modes:
- Full Mode: With database access (local or Render paid tier with persistent disk)
- Log-Only Mode: Without database (Render free tier) - logs events for later sync

Webhook Events Handled:
- contact_add: New contact created in AC
- contact_update: Profile fields updated in AC
- contact_delete: Contact removed (logged but no action per requirements)

Profile Fields Synced (AC wins):
- First Name, Last Name, Email, Phone
- Company, Industry, Job Title, Location, Professional Summary

Author: Scott Raven (via Claude Code)
Created: November 1, 2025
Updated: November 2, 2025 - Added free-tier support
"""

from flask import Flask, request, jsonify
import sqlite3
import json
import os
import hmac
import hashlib
from datetime import datetime
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('webhook_receiver.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configuration
DB_PATH = os.environ.get('DATABASE_PATH', '/Users/scottraven/Analytics/db/unified_analysis.db')
WEBHOOK_SECRET = os.environ.get('AC_WEBHOOK_SECRET', '')  # Set in Render environment
DB_AVAILABLE = os.path.exists(DB_PATH)  # Check if database is available (local only)

# Field mapping: ActiveCampaign field → our database field
AC_FIELD_MAPPING = {
    'firstName': 'first_name',
    'lastName': 'last_name',
    'email': 'primary_email',
    'phone': 'phone',
    # Custom fields in AC (will be in fieldValues)
    'company_name': 'company',
    'industry': 'industry',
    'job_title': 'position',
    'location': 'location',
    'professional_summary': 'professional_summary'
}


class WebhookProcessor:
    """Process ActiveCampaign webhook events"""

    def __init__(self, db_path: str):
        self.db_path = db_path

    def log_webhook(self, webhook_type: str, payload: dict, processed: bool = False,
                   person_id: int = None, error: str = None) -> int:
        """Log webhook event to database (or just logs if DB unavailable)"""

        ac_contact_id = self._extract_contact_id(payload)
        email = self._extract_email(payload)

        # If database unavailable (Render free tier), just log to console
        if not DB_AVAILABLE:
            logger.info(f"📝 Webhook logged (DB unavailable): {webhook_type} | AC#{ac_contact_id} | {email}")
            return None

        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                cursor.execute("""
                    INSERT INTO ac_webhook_log (
                        webhook_type, ac_contact_id, email,
                        received_at, payload, processed, person_id, error_message
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    webhook_type, ac_contact_id, email,
                    datetime.now().isoformat(), json.dumps(payload),
                    processed, person_id, error
                ))

                conn.commit()
                return cursor.lastrowid

        except Exception as e:
            logger.error(f"Failed to log webhook: {e}")
            return None

    def _extract_contact_id(self, payload: dict) -> str:
        """Extract contact ID from webhook payload"""
        contact = payload.get('contact', {})
        return contact.get('id', '') or payload.get('contact_id', '')

    def _extract_email(self, payload: dict) -> str:
        """Extract email from webhook payload"""
        contact = payload.get('contact', {})
        return contact.get('email', '').lower().strip()

    def find_person_by_ac_id(self, ac_contact_id: str) -> dict:
        """Find person in unified_persons by ac_contact_id"""

        if not DB_AVAILABLE:
            return None

        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()

                result = cursor.execute("""
                    SELECT person_id, name, primary_email, ac_contact_id
                    FROM unified_persons
                    WHERE ac_contact_id = ?
                    LIMIT 1
                """, (ac_contact_id,)).fetchone()

                if result:
                    return dict(result)
                return None

        except Exception as e:
            logger.error(f"Error finding person by AC ID: {e}")
            return None

    def find_person_by_email(self, email: str) -> dict:
        """Find person in unified_persons by email"""

        if not DB_AVAILABLE:
            return None

        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()

                result = cursor.execute("""
                    SELECT person_id, name, primary_email, ac_contact_id
                    FROM unified_persons
                    WHERE LOWER(TRIM(primary_email)) = ?
                    LIMIT 1
                """, (email.lower().strip(),)).fetchone()

                if result:
                    return dict(result)
                return None

        except Exception as e:
            logger.error(f"Error finding person by email: {e}")
            return None

    def extract_profile_fields(self, payload: dict) -> dict:
        """Extract profile fields from webhook payload"""

        contact = payload.get('contact', {})
        field_values = contact.get('fieldValues', [])

        # Support both camelCase (JSON) and snake_case (form-encoded)
        profile = {
            'first_name': contact.get('firstName') or contact.get('first_name', ''),
            'last_name': contact.get('lastName') or contact.get('last_name', ''),
            'email': contact.get('email', '').lower().strip(),
            'phone': contact.get('phone', ''),
        }

        # Extract custom fields
        for field_data in field_values:
            field_name = field_data.get('field', '')
            field_value = field_data.get('value', '')

            # Map AC custom field names to our fields
            if 'company' in field_name.lower():
                profile['company'] = field_value
            elif 'industry' in field_name.lower():
                profile['industry'] = field_value
            elif 'job' in field_name.lower() or 'title' in field_name.lower():
                profile['position'] = field_value
            elif 'location' in field_name.lower():
                profile['location'] = field_value
            elif 'summary' in field_name.lower():
                profile['professional_summary'] = field_value

        return profile

    def update_person_profile(self, person_id: int, ac_contact_id: str,
                            profile_updates: dict) -> bool:
        """Update person's profile fields in unified_persons"""

        if not DB_AVAILABLE:
            logger.info(f"✓ Would update person {person_id} with: {profile_updates}")
            return True  # Return success for AC webhook acknowledgment

        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                # Get current values for comparison
                current = cursor.execute("""
                    SELECT name, primary_email, phone, company, position,
                           industry, location, professional_summary
                    FROM unified_persons
                    WHERE person_id = ?
                """, (person_id,)).fetchone()

                if not current:
                    logger.warning(f"Person {person_id} not found")
                    return False

                # Build update query dynamically for changed fields
                updates = []
                params = []
                changes_logged = []

                # Combine first + last name
                if profile_updates.get('first_name') or profile_updates.get('last_name'):
                    new_name = f"{profile_updates.get('first_name', '')} {profile_updates.get('last_name', '')}".strip()
                    if new_name and new_name != current[0]:
                        updates.append("name = ?")
                        params.append(new_name)
                        changes_logged.append(('name', current[0], new_name))

                # Email
                if profile_updates.get('email') and profile_updates['email'] != current[1]:
                    updates.append("primary_email = ?")
                    params.append(profile_updates['email'])
                    changes_logged.append(('primary_email', current[1], profile_updates['email']))

                # Phone
                if profile_updates.get('phone') and profile_updates['phone'] != current[2]:
                    updates.append("phone = ?")
                    params.append(profile_updates['phone'])
                    changes_logged.append(('phone', current[2], profile_updates['phone']))

                # Company
                if profile_updates.get('company') and profile_updates['company'] != current[3]:
                    updates.append("company = ?")
                    params.append(profile_updates['company'])
                    changes_logged.append(('company', current[3], profile_updates['company']))

                # Position
                if profile_updates.get('position') and profile_updates['position'] != current[4]:
                    updates.append("position = ?")
                    params.append(profile_updates['position'])
                    changes_logged.append(('position', current[4], profile_updates['position']))

                # Industry
                if profile_updates.get('industry') and profile_updates['industry'] != current[5]:
                    updates.append("industry = ?")
                    params.append(profile_updates['industry'])
                    changes_logged.append(('industry', current[5], profile_updates['industry']))

                # Location
                if profile_updates.get('location') and profile_updates['location'] != current[6]:
                    updates.append("location = ?")
                    params.append(profile_updates['location'])
                    changes_logged.append(('location', current[6], profile_updates['location']))

                # Professional Summary
                if profile_updates.get('professional_summary') and profile_updates['professional_summary'] != current[7]:
                    updates.append("professional_summary = ?")
                    params.append(profile_updates['professional_summary'])
                    changes_logged.append(('professional_summary', current[7], profile_updates['professional_summary']))

                if updates:
                    # Update unified_persons
                    updates.append("ac_last_synced = ?")
                    params.append(datetime.now().isoformat())
                    params.append(person_id)

                    cursor.execute(f"""
                        UPDATE unified_persons
                        SET {', '.join(updates)}
                        WHERE person_id = ?
                    """, params)

                    # Log each change to ac_profile_updates
                    for field_name, old_val, new_val in changes_logged:
                        cursor.execute("""
                            INSERT INTO ac_profile_updates (
                                person_id, ac_contact_id, field_name,
                                old_value, new_value, source
                            ) VALUES (?, ?, ?, ?, ?, 'webhook')
                        """, (person_id, ac_contact_id, field_name, old_val, new_val))

                    conn.commit()

                    logger.info(f"✓ Updated {len(changes_logged)} fields for person {person_id}")
                    return True
                else:
                    logger.info(f"No changes needed for person {person_id}")
                    return True

        except Exception as e:
            logger.error(f"Error updating person profile: {e}")
            return False

    def handle_contact_update(self, payload: dict) -> dict:
        """Handle contact_update webhook event"""

        ac_contact_id = self._extract_contact_id(payload)
        email = self._extract_email(payload)

        logger.info(f"Processing contact_update for AC#{ac_contact_id} ({email})")

        # Find person by ac_contact_id
        person = self.find_person_by_ac_id(ac_contact_id)

        if not person:
            # Try finding by email
            person = self.find_person_by_email(email)

            if person and DB_AVAILABLE:
                # Link the ac_contact_id
                logger.info(f"Linking email {email} to AC#{ac_contact_id}")
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute("""
                        UPDATE unified_persons
                        SET ac_contact_id = ?, ac_profile_source = 'activecampaign'
                        WHERE person_id = ?
                    """, (ac_contact_id, person['person_id']))
                    conn.commit()

        if person:
            # Extract profile updates
            profile_updates = self.extract_profile_fields(payload)

            # Update the person
            success = self.update_person_profile(
                person['person_id'], ac_contact_id, profile_updates
            )

            self.log_webhook('contact_update', payload, processed=True,
                           person_id=person['person_id'])

            return {
                'success': success,
                'person_id': person['person_id'],
                'ac_contact_id': ac_contact_id,
                'message': 'Profile updated' if success else 'Update failed'
            }
        else:
            # Person not found - could be DB unavailable or actually not in database
            if not DB_AVAILABLE:
                logger.info(f"✓ Webhook received for AC#{ac_contact_id} (DB unavailable on Render free tier)")
                return {
                    'success': True,
                    'ac_contact_id': ac_contact_id,
                    'message': 'Webhook logged (database unavailable on free tier)'
                }
            else:
                # Person not found - log but don't create (per Q3: only add if in AC export)
                logger.warning(f"Contact AC#{ac_contact_id} not found in analytics database")
                self.log_webhook('contact_update', payload, processed=False,
                               error='Contact not in analytics database')

                return {
                    'success': False,
                    'ac_contact_id': ac_contact_id,
                    'message': 'Contact not found in analytics database'
                }

    def handle_contact_add(self, payload: dict) -> dict:
        """Handle contact_add webhook event"""

        ac_contact_id = self._extract_contact_id(payload)
        email = self._extract_email(payload)

        logger.info(f"Processing contact_add for AC#{ac_contact_id} ({email})")

        # Check if already exists
        person = self.find_person_by_ac_id(ac_contact_id)

        if not person:
            person = self.find_person_by_email(email)

        if person:
            # Already exists - just link the ac_contact_id if not set
            if not person['ac_contact_id'] and DB_AVAILABLE:
                logger.info(f"Linking existing person {person['person_id']} to AC#{ac_contact_id}")
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute("""
                        UPDATE unified_persons
                        SET ac_contact_id = ?, ac_profile_source = 'activecampaign'
                        WHERE person_id = ?
                    """, (ac_contact_id, person['person_id']))
                    conn.commit()

            self.log_webhook('contact_add', payload, processed=True,
                           person_id=person['person_id'])

            return {
                'success': True,
                'person_id': person['person_id'],
                'ac_contact_id': ac_contact_id,
                'message': 'Linked to existing person'
            }
        else:
            # No person found - could be DB unavailable or actually not in database
            if not DB_AVAILABLE:
                logger.info(f"✓ Webhook received for new contact AC#{ac_contact_id} (DB unavailable)")
                return {
                    'success': True,
                    'ac_contact_id': ac_contact_id,
                    'message': 'Webhook logged (database unavailable on free tier)'
                }
            else:
                # Per Q3: Contact not in our database yet - will be added via AC export later
                logger.info(f"New AC contact #{ac_contact_id} - will sync via export")
                self.log_webhook('contact_add', payload, processed=False,
                               error='New contact - pending export sync')

                return {
                    'success': False,
                    'ac_contact_id': ac_contact_id,
                    'message': 'New contact - will sync via next export'
                }

    def handle_contact_delete(self, payload: dict) -> dict:
        """Handle contact_delete webhook event"""

        ac_contact_id = self._extract_contact_id(payload)

        logger.info(f"Processing contact_delete for AC#{ac_contact_id}")

        # Per Q4: Leave unchanged - just log the event
        self.log_webhook('contact_delete', payload, processed=True)

        return {
            'success': True,
            'ac_contact_id': ac_contact_id,
            'message': 'Deletion logged (no action taken per requirements)'
        }


# Initialize processor
processor = WebhookProcessor(DB_PATH)


def verify_webhook_signature(request_data: bytes, signature: str) -> bool:
    """Verify webhook signature from ActiveCampaign"""

    if not WEBHOOK_SECRET:
        logger.warning("No webhook secret configured - skipping signature verification")
        return True  # Allow in development

    expected_signature = hmac.new(
        WEBHOOK_SECRET.encode('utf-8'),
        request_data,
        hashlib.sha256
    ).hexdigest()

    return hmac.compare_digest(signature, expected_signature)


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint for Render"""
    import os
    from pathlib import Path

    # Check database and disk status
    db_status = {
        'configured_path': DB_PATH,
        'path_exists': os.path.exists(DB_PATH),
        'parent_exists': os.path.exists(os.path.dirname(DB_PATH)),
        'is_writable': False
    }

    # Check if parent directory is writable
    parent_dir = os.path.dirname(DB_PATH)
    if os.path.exists(parent_dir):
        db_status['is_writable'] = os.access(parent_dir, os.W_OK)

    # Check if /var/data exists (persistent disk mount point)
    if os.path.exists('/var/data'):
        db_status['mount_point_exists'] = True
        db_status['mount_point_writable'] = os.access('/var/data', os.W_OK)
        try:
            db_status['mount_point_contents'] = os.listdir('/var/data')
        except:
            db_status['mount_point_contents'] = 'error listing'
    else:
        db_status['mount_point_exists'] = False

    return jsonify({
        'status': 'healthy',
        'service': 'activecampaign-webhook-receiver',
        'timestamp': datetime.now().isoformat(),
        'database_available': DB_AVAILABLE,
        'mode': 'full' if DB_AVAILABLE else 'log-only (free tier)',
        'database': db_status
    })


@app.route('/webhook/activecampaign', methods=['POST'])
def webhook_handler():
    """Main webhook endpoint"""

    try:
        # Verify signature if secret is configured
        signature = request.headers.get('X-AC-Signature', '')
        if WEBHOOK_SECRET and not verify_webhook_signature(request.data, signature):
            logger.warning("Invalid webhook signature")
            return jsonify({'error': 'Invalid signature'}), 401

        # Parse payload - ActiveCampaign might send as form data or JSON
        content_type = request.headers.get('Content-Type', '')

        if 'application/json' in content_type:
            payload = request.json
        elif 'application/x-www-form-urlencoded' in content_type:
            # AC sends form-encoded data with 'contact[field]' format
            payload = {
                'type': request.form.get('type', 'contact_update'),
                'contact': {}
            }

            # Extract contact fields from form data
            for key, value in request.form.items():
                if key.startswith('contact['):
                    field_name = key.replace('contact[', '').replace(']', '')
                    payload['contact'][field_name] = value
                elif key == 'type':
                    payload['type'] = value

            logger.info(f"Parsed form-encoded payload: {payload}")
        else:
            # Try to parse as JSON anyway
            try:
                payload = request.get_json(force=True)
            except:
                logger.error(f"Unsupported content type: {content_type}")
                logger.error(f"Request data: {request.data}")
                return jsonify({'error': 'Unsupported content type'}), 415

        webhook_type = payload.get('type', '')

        # Normalize webhook type - ActiveCampaign sends 'update', 'add', 'delete'
        # but we expect 'contact_update', 'contact_add', 'contact_delete'
        if webhook_type and not webhook_type.startswith('contact_'):
            webhook_type = f'contact_{webhook_type}'

        logger.info(f"Received webhook: {webhook_type}")

        # Route to appropriate handler
        if webhook_type == 'contact_update':
            result = processor.handle_contact_update(payload)
        elif webhook_type == 'contact_add':
            result = processor.handle_contact_add(payload)
        elif webhook_type == 'contact_delete':
            result = processor.handle_contact_delete(payload)
        else:
            logger.warning(f"Unknown webhook type: {webhook_type}")
            return jsonify({'error': 'Unknown webhook type'}), 400

        return jsonify(result), 200

    except Exception as e:
        logger.error(f"Webhook processing error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/', methods=['GET'])
def index():
    """Root endpoint"""
    return jsonify({
        'service': 'ActiveCampaign Webhook Receiver',
        'status': 'running',
        'endpoints': {
            'health': '/health',
            'webhook': '/webhook/activecampaign',
            'api': '/api/profile-updates'
        }
    })


@app.route('/api/profile-updates', methods=['GET'])
def api_profile_updates():
    """API endpoint to fetch profile updates for sync"""

    if not DB_AVAILABLE:
        return jsonify({
            'error': 'Database not available (free tier mode)',
            'updates': []
        }), 503

    try:
        # Get optional query parameters
        since = request.args.get('since')  # ISO datetime string
        limit = request.args.get('limit', 1000, type=int)
        include_synced = request.args.get('include_synced', 'false').lower() == 'true'

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Ensure synced_to_local column exists (migration)
        try:
            cursor.execute("SELECT synced_to_local FROM ac_profile_updates LIMIT 1")
        except sqlite3.OperationalError:
            logger.info("Adding synced_to_local column to ac_profile_updates table")
            cursor.execute("ALTER TABLE ac_profile_updates ADD COLUMN synced_to_local INTEGER DEFAULT 0")
            cursor.execute("ALTER TABLE ac_profile_updates ADD COLUMN synced_at TEXT")
            conn.commit()

        # Build query
        query = """
            SELECT
                update_id,
                person_id,
                ac_contact_id,
                field_name,
                old_value,
                new_value,
                updated_at,
                source
            FROM ac_profile_updates
            WHERE 1=1
        """

        params = []

        # Filter by sync status (default: only unsynced)
        if not include_synced:
            query += " AND (synced_to_local IS NULL OR synced_to_local = 0)"

        if since:
            query += " AND updated_at >= ?"
            params.append(since)

        query += " ORDER BY updated_at DESC LIMIT ?"
        params.append(limit)

        # Fetch updates
        updates = []
        for row in cursor.execute(query, params):
            updates.append({
                'update_id': row[0],
                'person_id': row[1],
                'ac_contact_id': row[2],
                'field_name': row[3],
                'old_value': row[4],
                'new_value': row[5],
                'updated_at': row[6],
                'source': row[7]
            })

        conn.close()

        return jsonify({
            'success': True,
            'count': len(updates),
            'updates': updates
        })

    except Exception as e:
        logger.error(f"API error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/profile-updates/confirm', methods=['POST'])
def confirm_sync():
    """Confirm that updates have been synced to local database"""

    if not DB_AVAILABLE:
        return jsonify({'error': 'Database not available'}), 503

    try:
        data = request.get_json()
        update_ids = data.get('update_ids', [])

        if not update_ids:
            return jsonify({'error': 'No update_ids provided'}), 400

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Mark updates as synced
        placeholders = ','.join('?' * len(update_ids))
        cursor.execute(f"""
            UPDATE ac_profile_updates
            SET synced_to_local = 1,
                synced_at = ?
            WHERE update_id IN ({placeholders})
        """, [datetime.now().isoformat()] + update_ids)

        updated_count = cursor.rowcount
        conn.commit()
        conn.close()

        logger.info(f"Marked {updated_count} updates as synced: {update_ids}")

        return jsonify({
            'success': True,
            'updates_confirmed': updated_count
        })

    except Exception as e:
        logger.error(f"Sync confirmation error: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/profile-updates/cleanup', methods=['POST'])
def cleanup_old_updates():
    """Delete synced updates older than specified days"""

    if not DB_AVAILABLE:
        return jsonify({'error': 'Database not available'}), 503

    try:
        data = request.get_json() or {}
        days_old = data.get('days_old', 30)  # Default: 30 days
        dry_run = data.get('dry_run', True)  # Default: dry run

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        from datetime import timedelta
        cutoff_date = (datetime.now() - timedelta(days=days_old)).isoformat()

        # Count how many would be deleted
        count_query = """
            SELECT COUNT(*) FROM ac_profile_updates
            WHERE synced_to_local = 1
            AND synced_at < ?
        """
        count = cursor.execute(count_query, (cutoff_date,)).fetchone()[0]

        if not dry_run:
            # Actually delete
            cursor.execute("""
                DELETE FROM ac_profile_updates
                WHERE synced_to_local = 1
                AND synced_at < ?
            """, (cutoff_date,))
            conn.commit()
            logger.info(f"Deleted {count} old synced updates (older than {days_old} days)")

        conn.close()

        return jsonify({
            'success': True,
            'would_delete' if dry_run else 'deleted': count,
            'cutoff_date': cutoff_date,
            'dry_run': dry_run
        })

    except Exception as e:
        logger.error(f"Cleanup error: {e}")
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
