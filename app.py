import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, avg, count
import pandas as pd
import matplotlib.pyplot as plt
import os

# ==========================================================
# 1️⃣ Initialize Spark
# ==========================================================
spark = SparkSession.builder \
    .appName("Online Bookstore Insights") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# ==========================================================
# 2️⃣ Load Dataset from Supabase
# ==========================================================
@st.cache_data
def load_data():
    # 🔗 Supabase public CSV URL
    supabase_url = "https://caiqunybkbwilxmceyyj.supabase.co/storage/v1/object/public/bookstore/books.csv"

    try:
        pdf = pd.read_csv(supabase_url)
        return pdf
    except Exception as e:
        st.error(f"❌ Failed to load data from Supabase: {e}")
        return pd.DataFrame()

pdf = load_data()

if not pdf.empty:
    df = spark.createDataFrame(pdf)
else:
    st.stop()  # stop app if dataset missing

# ==========================================================
# 3️⃣ Sidebar Navigation
# ==========================================================
st.sidebar.title("📚 Online Bookstore Insights")
page = st.sidebar.radio(
    "Navigate to",
    ["🏠 Home", "📊 Dashboard", "🔍 Search", "🏆 Top Rated Books",
     "📈 Insights", "📥 Upload Data", "💬 Feedback"]
)

# ==========================================================
# 🏠 HOME
# ==========================================================
if page == "🏠 Home":
    st.title("📚 Online Bookstore Data Insights")
    st.markdown("""
    Welcome to **Online Bookstore Insights**, a PySpark + Streamlit dashboard
    for exploring and analyzing book datasets efficiently.

    ### 🔹 Features:
    - Average ratings and summary metrics
    - Search books by title or author
    - Top-rated and bestselling books
    - Author-based insights
    - Upload and explore your own dataset
    """)

# ==========================================================
# 📊 DASHBOARD
# ==========================================================
elif page == "📊 Dashboard":
    st.title("📊 Book Ratings Dashboard")

    st.subheader("Dataset Preview")
    st.dataframe(pdf.head(10))

    st.subheader("📈 Summary Statistics")
    avg_rating = df.select(avg(col("average_rating"))).collect()[0][0]
    total_books = df.count()
    total_authors = df.select("authors").distinct().count()

    bestseller_col = "ratings_count" if "ratings_count" in df.columns else None
    bestseller_count = df.select(count(col(bestseller_col))).collect()[0][0] if bestseller_col else "N/A"

    c1, c2, c3 = st.columns(3)
    c1.metric("⭐ Average Rating", f"{avg_rating:.2f}")
    c2.metric("📚 Total Books", total_books)
    c3.metric("🏆 Bestsellers", str(bestseller_count))

# ==========================================================
# 🔍 SEARCH
# ==========================================================
elif page == "🔍 Search":
    st.title("🔍 Search Books by Name or Author")

    search = st.text_input("Enter book title or author name:")
    if search:
        results = df.filter(
            (col("title").like(f"%{search}%")) |
            (col("authors").like(f"%{search}%"))
        )
        st.write(f"Results for: **{search}**")
        st.dataframe(results.limit(20).toPandas())

# ==========================================================
# 🏆 TOP RATED BOOKS
# ==========================================================
elif page == "🏆 Top Rated Books":
    st.title("🏆 Top Rated Books")
    top_books = df.orderBy(desc("average_rating")).limit(20)
    st.dataframe(top_books.toPandas())

    st.subheader("Top 10 Books by Rating")
    top_pd = top_books.limit(10).toPandas()
    plt.figure(figsize=(10, 5))
    plt.barh(top_pd["title"], top_pd["average_rating"], color="skyblue")
    plt.xlabel("Average Rating")
    plt.ylabel("Book Title")
    plt.gca().invert_yaxis()
    st.pyplot(plt)

# ==========================================================
# 📈 INSIGHTS
# ==========================================================
elif page == "📈 Insights":
    st.title("📈 Analytical Insights")

    st.subheader("👩‍💻 Top Authors by Number of Books")
    top_authors = df.groupBy("authors").agg(count("*").alias("book_count")).orderBy(desc("book_count")).limit(10)
    st.dataframe(top_authors.toPandas())

    top_pd = top_authors.toPandas()
    plt.figure(figsize=(10, 5))
    plt.barh(top_pd["authors"], top_pd["book_count"], color="lightcoral")
    plt.xlabel("Number of Books")
    plt.ylabel("Authors")
    plt.gca().invert_yaxis()
    st.pyplot(plt)

    st.subheader("⭐ Rating Distribution")
    plt.figure(figsize=(8, 4))
    pdf["average_rating"].hist(bins=20, color="mediumpurple")
    plt.xlabel("Average Rating")
    plt.ylabel("Frequency")
    st.pyplot(plt)

# ==========================================================
# 📥 UPLOAD DATA
# ==========================================================
elif page == "📥 Upload Data":
    st.title("📥 Upload a New CSV Dataset")

    uploaded = st.file_uploader("Choose a CSV file", type=["csv"])
    if uploaded is not None:
        new_df = spark.read.csv(uploaded, header=True, inferSchema=True)
        st.success("✅ File uploaded successfully!")
        st.write("Preview:")
        st.dataframe(new_df.limit(10).toPandas())

# ==========================================================
# 💬 FEEDBACK
# ==========================================================
elif page == "💬 Feedback":
    st.title("💬 Feedback Form")
    name = st.text_input("Your Name")
    feedback = st.text_area("Your Feedback")

    if st.button("Submit"):
        if name and feedback:
            st.success("Thank you for your feedback! 💚")
        else:
            st.warning("Please fill in all fields.")
