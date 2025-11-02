#!/usr/bin/env python3
"""
ActiveCampaign Webhook Receiver - Production Edition

Receives and processes ActiveCampaign webhook events for profile field updates.
Designed for deployment on Render.com with Github auto-deploy.

Webhook Events Handled:
- contact_add: New contact created in AC
- contact_update: Profile fields updated in AC
- contact_delete: Contact removed (logged but no action per requirements)

Profile Fields Synced (AC wins):
- First Name, Last Name, Email, Phone
- Company, Industry, Job Title, Location, Professional Summary

Author: Scott Raven (via Claude Code)
Created: November 1, 2025
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
        """Log webhook event to database"""

        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                ac_contact_id = self._extract_contact_id(payload)
                email = self._extract_email(payload)

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

        profile = {
            'first_name': contact.get('firstName', ''),
            'last_name': contact.get('lastName', ''),
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

            if person:
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
            if not person['ac_contact_id']:
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
    return jsonify({
        'status': 'healthy',
        'service': 'activecampaign-webhook-receiver',
        'timestamp': datetime.now().isoformat()
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

        # Parse payload
        payload = request.json
        webhook_type = payload.get('type', '')

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
            'webhook': '/webhook/activecampaign'
        }
    })


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
