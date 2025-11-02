#!/usr/bin/env python3
"""
Initialize minimal database for webhook receiver on Render.

This script creates a minimal database with just the tables and data
needed for the webhook receiver to function:
- unified_persons (just ac_contact_id mappings)
- ac_webhook_log
- ac_profile_updates

Usage:
    # Create minimal database from existing full database
    python3 init_webhook_database.py --source db/unified_analysis.db --output webhook_db.db

    # Then upload to Render using SSH or Render shell
"""

import sqlite3
import argparse
from pathlib import Path
from datetime import datetime


def create_minimal_database(source_db: str, output_db: str):
    """Create minimal database with webhook tables and contact mappings"""

    print(f"📦 Creating minimal webhook database from {source_db}...")

    # Remove existing output if it exists
    output_path = Path(output_db)
    if output_path.exists():
        output_path.unlink()
        print(f"   Removed existing {output_db}")

    # Connect to both databases
    source_conn = sqlite3.connect(source_db)
    output_conn = sqlite3.connect(output_db)

    try:
        # Create tables in output database
        output_cursor = output_conn.cursor()

        print("📋 Creating tables...")

        # 1. Create minimal unified_persons table (just fields needed for webhook)
        output_cursor.execute("""
            CREATE TABLE unified_persons (
                person_id INTEGER PRIMARY KEY,
                name TEXT,
                primary_email TEXT,
                company TEXT,
                position TEXT,
                ac_contact_id TEXT,
                ac_last_synced TEXT,
                ac_profile_source TEXT DEFAULT 'activecampaign'
            )
        """)

        # Create unique index on ac_contact_id
        output_cursor.execute("""
            CREATE UNIQUE INDEX idx_ac_contact_id_unique
            ON unified_persons(ac_contact_id)
            WHERE ac_contact_id IS NOT NULL
        """)

        print("   ✓ unified_persons")

        # 2. Create ac_webhook_log table
        output_cursor.execute("""
            CREATE TABLE ac_webhook_log (
                log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                webhook_type TEXT NOT NULL,
                ac_contact_id TEXT,
                email TEXT,
                received_at TEXT NOT NULL,
                payload TEXT,
                processed INTEGER DEFAULT 0,
                person_id INTEGER,
                error_message TEXT,
                FOREIGN KEY (person_id) REFERENCES unified_persons(person_id)
            )
        """)

        output_cursor.execute("""
            CREATE INDEX idx_webhook_log_contact ON ac_webhook_log(ac_contact_id)
        """)
        output_cursor.execute("""
            CREATE INDEX idx_webhook_log_received ON ac_webhook_log(received_at)
        """)

        print("   ✓ ac_webhook_log")

        # 3. Create ac_profile_updates table
        output_cursor.execute("""
            CREATE TABLE ac_profile_updates (
                update_id INTEGER PRIMARY KEY AUTOINCREMENT,
                person_id INTEGER NOT NULL,
                ac_contact_id TEXT NOT NULL,
                field_name TEXT NOT NULL,
                old_value TEXT,
                new_value TEXT,
                updated_at TEXT NOT NULL,
                source TEXT DEFAULT 'activecampaign',
                FOREIGN KEY (person_id) REFERENCES unified_persons(person_id)
            )
        """)

        output_cursor.execute("""
            CREATE INDEX idx_profile_updates_person ON ac_profile_updates(person_id)
        """)
        output_cursor.execute("""
            CREATE INDEX idx_profile_updates_updated ON ac_profile_updates(updated_at)
        """)

        print("   ✓ ac_profile_updates")

        # 4. Copy contact mappings from source database
        print("\n📥 Copying contact mappings...")

        source_cursor = source_conn.cursor()
        contacts = source_cursor.execute("""
            SELECT
                person_id,
                name,
                primary_email,
                company,
                position,
                ac_contact_id,
                ac_last_synced,
                ac_profile_source
            FROM unified_persons
            WHERE ac_contact_id IS NOT NULL
        """).fetchall()

        print(f"   Found {len(contacts)} contacts with ActiveCampaign IDs")

        output_cursor.executemany("""
            INSERT INTO unified_persons (
                person_id, name, primary_email, company, position,
                ac_contact_id, ac_last_synced, ac_profile_source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, contacts)

        print(f"   ✓ Copied {len(contacts)} contact mappings")

        # Commit changes
        output_conn.commit()

        # Show statistics
        print(f"\n✅ Minimal database created: {output_db}")
        print(f"   Size: {output_path.stat().st_size / 1024:.1f} KB")
        print(f"   Contacts: {len(contacts)}")

        # Verify tables
        tables = output_cursor.execute("""
            SELECT name FROM sqlite_master WHERE type='table' ORDER BY name
        """).fetchall()

        print(f"\n📊 Tables in minimal database:")
        for table in tables:
            count = output_cursor.execute(f"SELECT COUNT(*) FROM {table[0]}").fetchone()[0]
            print(f"   - {table[0]}: {count} rows")

        print(f"\n🚀 Ready to upload to Render at: /var/data/unified_analysis.db")

    finally:
        source_conn.close()
        output_conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Create minimal database for webhook receiver"
    )
    parser.add_argument(
        '--source',
        default='db/unified_analysis.db',
        help='Source database path (default: db/unified_analysis.db)'
    )
    parser.add_argument(
        '--output',
        default='webhook_db.db',
        help='Output database path (default: webhook_db.db)'
    )

    args = parser.parse_args()

    # Check source exists
    if not Path(args.source).exists():
        print(f"❌ Source database not found: {args.source}")
        return 1

    create_minimal_database(args.source, args.output)

    print("\n📝 Next steps:")
    print("   1. Upload webhook_db.db to Render persistent disk")
    print("   2. Use Render Shell: Settings → Shell → Upload file")
    print("   3. Move to: /var/data/unified_analysis.db")
    print("   4. Or use SCP/SFTP if available")

    return 0


if __name__ == '__main__':
    exit(main())
