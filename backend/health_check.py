import os
import sys

def check_env():
    required_vars = [
        "AIRTABLE_TOKEN",
        "AIRTABLE_BASE_ID",
        "POLYGON_API_KEY",
        "ZAPIER_WEBHOOK_URL",
        "RUN_SECRET"
    ]
    
    frontend_vars = [
        "NUXT_PUBLIC_API_BASE"
    ]
    
    print("=== ENVIRONMENT DIAGNOSTIC ===")
    
    all_ok = True
    
    print("\n--- Backend Requirements ---")
    for var in required_vars:
        val = os.getenv(var)
        if val:
            print(f"[OK]   {var} is set (length: {len(val)})")
        else:
            print(f"[FAIL] {var} is MISSING")
            all_ok = False
            
    print("\n--- Frontend Requirements ---")
    for var in frontend_vars:
        val = os.getenv(var)
        if val:
            print(f"[OK]   {var} is set: {val}")
        else:
            print(f"[WARN] {var} is MISSING (Note: Only needed on the Frontend service)")
            
    print("\n" + "="*30)
    if all_ok:
        print("RESULT: Backend environment is READY.")
    else:
        print("RESULT: Backend environment is MISSING CRITICAL KEYS.")
        sys.exit(1)

if __name__ == "__main__":
    check_env()
