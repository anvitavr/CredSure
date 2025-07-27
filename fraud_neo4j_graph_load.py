from neo4j import GraphDatabase
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# ---------------------------
# Neo4j Connection Config
# ---------------------------
uri = "bolt://127.0.0.1:7687"
username = "neo4j"
password = "12345678"

driver = GraphDatabase.driver(uri, auth=(username, password))

# ---------------------------
# Helper Functions
# ---------------------------
def clear_graph(session):
    session.run("MATCH (n) DETACH DELETE n")

def create_customers(session):
    customers = [
        {"id": "C001", "name": "Alice Grant", "email": "alice.grant@example.uk"},
        {"id": "C002", "name": "John Wade", "email": "john.wade@example.uk"},
        {"id": "C003", "name": "Priya Desai", "email": "priya.desai@example.uk"}
    ]
    for c in customers:
        session.run("""
            CREATE (c:Customer {
                customer_id: $id,
                name: $name,
                email: $email
            })
        """, id=c["id"], name=c["name"], email=c["email"])

def create_devices(session):
    devices = [
        {"id": "D001", "type": "Mobile", "ip": "192.168.1.10"},
        {"id": "D002", "type": "Laptop", "ip": "10.0.0.2"},
        {"id": "D003", "type": "Tablet", "ip": "172.16.0.5"}
    ]
    for d in devices:
        session.run("""
            CREATE (d:Device {
                device_id: $id,
                device_type: $type,
                ip_address: $ip
            })
        """, id=d["id"], type=d["type"], ip=d["ip"])

def create_customer_device_links(session):
    links = [
        {"cid": "C001", "did": "D001"},
        {"cid": "C002", "did": "D002"},
        {"cid": "C003", "did": "D003"}
    ]
    for l in links:
        session.run("""
            MATCH (c:Customer {customer_id: $cid}), (d:Device {device_id: $did})
            CREATE (c)-[:USES]->(d)
        """, cid=l["cid"], did=l["did"])

def create_transactions(session):
    transactions = [
        {"id": "T001", "amount": 5000, "merchant": "Amazon", "location": "NY", "device_id": "D001"},
        {"id": "T002", "amount": 7000, "merchant": "eBay", "location": "CA", "device_id": "D002"},
        {"id": "T003", "amount": 9000, "merchant": "Walmart", "location": "TX", "device_id": "D003"}
    ]
    for txn in transactions:
        session.run("""
            MATCH (d:Device {device_id: $device_id})
            CREATE (t:Transaction {
                transaction_id: $id,
                amount: $amount,
                merchant: $merchant,
                location: $location,
                timestamp: datetime()
            })
            CREATE (d)-[:USED_IN]->(t)
        """, **txn)

def create_alerts(session):
    alerts = [
        {"id": "A001", "severity": "Moderate", "description": "Location mismatch"},
        {"id": "A002", "severity": "High", "description": "High-value transaction"},
        {"id": "A003", "severity": "Critical", "description": "High amount + flagged location"}
    ]
    for i, alert in enumerate(alerts):
        session.run("""
            MATCH (t:Transaction {transaction_id: $tid})
            CREATE (a:Alert {
                alert_id: $id,
                severity: $severity,
                description: $description,
                created_at: datetime()
            })
            CREATE (t)-[:TRIGGERED]->(a)
        """, tid=f"T00{i+1}", **alert)

def most_flagged_customers(session):
    print("\nMost Flagged Customers:")
    result = session.run("""
        MATCH (c:Customer)-[:USES]->(:Device)-[:USED_IN]->(:Transaction)-[:TRIGGERED]->(a:Alert)
        RETURN c.name AS name, COUNT(a) AS alerts
        ORDER BY alerts DESC
        LIMIT 5
    """)
    for row in result:
        print(f"{row['name']} - {row['alerts']} alerts")

def alert_distribution(session):
    print("\nAlert Distribution by Severity:")
    result = session.run("""
        MATCH (:Transaction)-[:TRIGGERED]->(a:Alert)
        RETURN a.severity AS severity, COUNT(*) AS count
    """)
    for row in result:
        print(f"{row['severity']}: {row['count']}")

def visualize_query():
    return """
    MATCH (c:Customer)-[:USES]->(d:Device)-[:USED_IN]->(t:Transaction)-[:TRIGGERED]->(a:Alert)
    RETURN c, d, t, a
    """

def fetch_alert_data():
    with driver.session() as session:
        result = session.run("""
            MATCH (c:Customer)-[:USES]->(d:Device)-[:USED_IN]->(t:Transaction)-[:TRIGGERED]->(a:Alert)
            RETURN a.severity AS severity, a.created_at AS created_at, t.amount AS amount,
                   t.merchant AS merchant, t.location AS location
        """)
        records = [dict(record) for record in result]
        for r in records:
            r["created_at"] = str(r["created_at"])
        return pd.DataFrame(records)

def plot_charts(df):
    if df.empty:
        print("No data to plot.")
        return

    sns.set(style="whitegrid")

    # Bar chart: Severity distribution
    plt.figure(figsize=(6,4))
    sns.countplot(data=df, x='severity', order=['Moderate', 'High', 'Critical'])
    plt.title('Alerts by Severity')
    plt.tight_layout()
    plt.show()

    # Line chart: Alerts over time
    df['created_at'] = pd.to_datetime(df['created_at'])
    df['date'] = df['created_at'].dt.date
    trend = df.groupby('date').size().reset_index(name='count')

    plt.figure(figsize=(7,4))
    sns.lineplot(data=trend, x='date', y='count', marker='o')
    plt.title('Alerts Over Time')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

    # Heatmap: Merchant vs Location
    pivot = df.pivot_table(index='merchant', columns='location', values='severity', aggfunc='count', fill_value=0)
    plt.figure(figsize=(7,5))
    sns.heatmap(pivot, annot=True, fmt='d', cmap='YlGnBu')
    plt.title('Alert Count by Merchant and Location')
    plt.tight_layout()
    plt.show()

# ---------------------------
# Main Execution
# ---------------------------
if __name__ == "__main__":
    with driver.session() as session:
        clear_graph(session)
        create_customers(session)
        create_devices(session)
        create_customer_device_links(session)
        create_transactions(session)
        create_alerts(session)
        most_flagged_customers(session)
        alert_distribution(session)

    print("\nFraud graph data loaded into Neo4j.")
    print("\nRun this in Neo4j Browser to visualize:")
    print(visualize_query())

    df = fetch_alert_data()
    plot_charts(df)

    driver.close()
