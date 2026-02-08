# ==================================================================================
# Validation Queries for wias1 and wics1
# Compare with expected DAX measures from Power BI
# ==================================================================================

"""
Based on your DAX formulas from the images, here are the SQL equivalents:

Expected Results from your notes (Image 10):
- wealthcustomer = CALCULATE(DISTINCTCOUNT(Wealth[pw1.rcifnumber])) → like 266k
- Topof company Digital Active = CALCULATE(DISTINCTCOUNT(Digital[rcifnumber]), FILTER('Digital', 'Digital'[digitallyactiveflag] = "Digital Active")) → like 3.42m
- DigitalEnrollment Wealth = calculate(Wealth[wealthcustomer], 'RCIF'[rcifdig.digitalflag] = "Digital User") → like 12k
- Accounts = COUNT(Wealth[pw1.acctscnt]) → like 303k
- account Accountper user = [Accounts]/Wealth[wealthcustomer] → 6.5
- WealthDigitallyActive Penentration = DIVIDE(CALCULATE(DISTINCTCOUNT(Wealth[pw1.rcifnumber]), INTERSECT(VALUES(Wealth[pw1.rcifnumber]), CALCULATETABLE(VALUES(Digital[rcifcustomernbr]), Digital[digitallyactiveflag] = "Digital Active"))), DISTINCTCOUNT(Wealth[pw1.rcifnumber])) → like 33ish
- DigitalWealth Penpen = DISTINCTCOUNT(Wealth[pw1.rcifnumber])/DISTINCTCOUNT('RCIF'[rcifdig.custinternetbankingnbr]) → like 3.3
"""

# ==================================================================================
# WIAS1 Validation Queries
# ==================================================================================

print("=" * 80)
print("WIAS1 (Customer Dimension) Validation Queries")
print("=" * 80)

# Query 1: Total Distinct Customers
print("\n1. Total Distinct Customers (RCIF_NUMBER):")
print("   Expected: ~31.8M (you got 31851632 ✓)")
spark.sql("""
    SELECT COUNT(DISTINCT rcif_number) as total_customers
    FROM dm_ib_dev.wias1
""").show()

# Query 2: Total Distinct IP_IDs  
print("\n2. Total Distinct IP_IDs:")
spark.sql("""
    SELECT COUNT(DISTINCT ip_id) as total_ip_ids
    FROM dm_ib_dev.wias1
""").show()

# Query 3: Wealth Customers (should be ~266k)
print("\n3. Wealth Customers (with Business Group):")
print("   Expected: ~266k")
spark.sql("""
    SELECT 
        COUNT(DISTINCT rcif_number) as wealth_customers,
        business_group
    FROM dm_ib_dev.wias1
    WHERE business_group IS NOT NULL
    GROUP BY business_group
    ORDER BY wealth_customers DESC
""").show()

# Query 4: Digital Active Customers (should be ~3.42M)
print("\n4. Digital Active Customers:")
print("   Expected: ~3.42M")
spark.sql("""
    SELECT 
        COUNT(DISTINCT rcif_number) as digital_active_customers
    FROM dm_ib_dev.wias1
    WHERE digitally_active = 'Digital Active'
""").show()

# Query 5: Digital Enrollment in Wealth (should be ~12k)
print("\n5. Wealth Customers who are Digital Users:")
print("   Expected: ~12k")
spark.sql("""
    SELECT 
        COUNT(DISTINCT rcif_number) as wealth_digital_users
    FROM dm_ib_dev.wias1
    WHERE business_group IS NOT NULL
        AND digital_user = 'Digital User'
""").show()

# Query 6: Breakdown by Business Group
print("\n6. Customer Count by Business Group:")
spark.sql("""
    SELECT 
        business_group,
        COUNT(DISTINCT rcif_number) as customer_count,
        COUNT(DISTINCT CASE WHEN digital_user = 'Digital User' THEN rcif_number END) as digital_users,
        COUNT(DISTINCT CASE WHEN digitally_active = 'Digital Active' THEN rcif_number END) as digital_active
    FROM dm_ib_dev.wias1
    WHERE business_group IS NOT NULL
    GROUP BY business_group
    ORDER BY customer_count DESC
""").show()

# Query 7: Breakdown by Division
print("\n7. Customer Count by Division:")
spark.sql("""
    SELECT 
        division,
        COUNT(DISTINCT rcif_number) as customer_count
    FROM dm_ib_dev.wias1
    WHERE division IS NOT NULL
    GROUP BY division
    ORDER BY customer_count DESC
""").show()

# Query 8: Generation Breakdown
print("\n8. Customer Count by Generation:")
spark.sql("""
    SELECT 
        generation,
        COUNT(DISTINCT rcif_number) as customer_count
    FROM dm_ib_dev.wias1
    GROUP BY generation
    ORDER BY customer_count DESC
""").show()

# Query 9: Digital Activity Flags Distribution
print("\n9. Digital Activity Distribution:")
spark.sql("""
    SELECT 
        'Mobile Active' as flag_type,
        mobile_active as flag_value,
        COUNT(DISTINCT rcif_number) as customer_count
    FROM dm_ib_dev.wias1
    WHERE mobile_active IS NOT NULL
    GROUP BY mobile_active
    
    UNION ALL
    
    SELECT 
        'OLB Active' as flag_type,
        olb_active as flag_value,
        COUNT(DISTINCT rcif_number) as customer_count
    FROM dm_ib_dev.wias1
    WHERE olb_active IS NOT NULL
    GROUP BY olb_active
    
    UNION ALL
    
    SELECT 
        'Digitally Active' as flag_type,
        digitally_active as flag_value,
        COUNT(DISTINCT rcif_number) as customer_count
    FROM dm_ib_dev.wias1
    WHERE digitally_active IS NOT NULL
    GROUP BY digitally_active
    
    ORDER BY flag_type, customer_count DESC
""").show(truncate=False)

# ==================================================================================
# WICS1 Validation Queries
# ==================================================================================

print("\n" + "=" * 80)
print("WICS1 (Account Fact Table) Validation Queries")
print("=" * 80)

# Query 10: Total Account Records
print("\n10. Total Account Records:")
spark.sql("""
    SELECT COUNT(*) as total_account_records
    FROM dm_ib_dev.wics1
""").show()

# Query 11: Total Accounts (sum of account_count) - Expected ~303k
print("\n11. Total Accounts:")
print("    Expected: ~303k")
spark.sql("""
    SELECT 
        SUM(account_count) as total_accounts,
        COUNT(DISTINCT rcif_number) as unique_customers
    FROM dm_ib_dev.wics1
""").show()

# Query 12: Accounts by Business Group
print("\n12. Accounts by Business Group:")
spark.sql("""
    SELECT 
        business_group,
        SUM(account_count) as total_accounts,
        COUNT(DISTINCT rcif_number) as unique_customers,
        ROUND(SUM(account_count) / COUNT(DISTINCT rcif_number), 2) as accounts_per_customer
    FROM dm_ib_dev.wics1
    GROUP BY business_group
    ORDER BY total_accounts DESC
""").show()

# Query 13: Accounts by Division
print("\n13. Accounts by Division:")
spark.sql("""
    SELECT 
        division,
        SUM(account_count) as total_accounts,
        COUNT(DISTINCT rcif_number) as unique_customers
    FROM dm_ib_dev.wics1
    GROUP BY division
    ORDER BY total_accounts DESC
""").show()

# Query 14: Account Type Breakdown
print("\n14. Account Type Counts:")
spark.sql("""
    SELECT 
        SUM(corporate_trust_cnt) as total_corporate_trust,
        SUM(institutional_trust_cnt) as total_institutional_trust,
        SUM(investment_cnt) as total_investment,
        SUM(insurance_cnt) as total_insurance,
        SUM(pwm_cnt) as total_pwm,
        SUM(trust_cnt) as total_trust,
        SUM(banking_cnt) as total_banking
    FROM dm_ib_dev.wics1
""").show()

# Query 15: Accounts per User (Expected ~6.5)
print("\n15. Accounts per User:")
print("    Expected: ~6.5")
spark.sql("""
    SELECT 
        ROUND(SUM(account_count) * 1.0 / COUNT(DISTINCT rcif_number), 2) as accounts_per_user
    FROM dm_ib_dev.wics1
""").show()

# ==================================================================================
# COMBINED Validation (JOIN wias1 and wics1)
# ==================================================================================

print("\n" + "=" * 80)
print("COMBINED Validation (Wealth Digital Penetration)")
print("=" * 80)

# Query 16: Wealth Digitally Active Penetration (Expected ~33%)
print("\n16. Wealth Digitally Active Penetration:")
print("    Expected: ~33%")
spark.sql("""
    SELECT 
        COUNT(DISTINCT CASE WHEN w.digitally_active = 'Digital Active' THEN w.rcif_number END) as wealth_digital_active,
        COUNT(DISTINCT w.rcif_number) as total_wealth_customers,
        ROUND(
            COUNT(DISTINCT CASE WHEN w.digitally_active = 'Digital Active' THEN w.rcif_number END) * 100.0 / 
            COUNT(DISTINCT w.rcif_number), 
            2
        ) as penetration_pct
    FROM dm_ib_dev.wias1 w
    WHERE w.business_group IS NOT NULL
""").show()

# Query 17: Digital Wealth Pen (Expected ~3.3)
print("\n17. Digital Wealth Penetration Ratio:")
print("    Expected: ~3.3")
spark.sql("""
    SELECT 
        COUNT(DISTINCT CASE WHEN business_group IS NOT NULL THEN rcif_number END) as wealth_customers,
        COUNT(DISTINCT CASE WHEN ibn IS NOT NULL THEN ibn END) as total_ibns,
        ROUND(
            COUNT(DISTINCT CASE WHEN business_group IS NOT NULL THEN rcif_number END) * 1.0 / 
            NULLIF(COUNT(DISTINCT CASE WHEN ibn IS NOT NULL THEN ibn END), 0),
            2
        ) as wealth_per_ibn_ratio
    FROM dm_ib_dev.wias1
""").show()

print("\n" + "=" * 80)
print("Validation Complete!")
print("=" * 80)
