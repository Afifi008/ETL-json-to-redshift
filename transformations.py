from pyspark.sql.functions import col, explode, element_at
# ====================================================================
# SILVER LAYER
# ====================================================================

def build_listings(df_json):
    return df_json.select(
        col("_id").alias("listing_id"),
        col("name"),
        col("property_type"),
        col("room_type"),
        col("bed_type"),
        col("minimum_nights").cast("int"),
        col("maximum_nights").cast("int"),
        col("accommodates.$numberInt").cast("int").alias("accommodates"),
        col("bedrooms.$numberInt").cast("int").alias("bedrooms"),
        col("beds.$numberInt").cast("int").alias("beds"),
        col("bathrooms.$numberDecimal").cast("double").alias("bathrooms"),
        col("price.$numberDecimal").cast("double").alias("price"),
        col("cleaning_fee.$numberDecimal").cast("double").alias("cleaning_fee"),
        col("security_deposit.$numberDecimal").cast("double").alias("security_deposit")
        #col("cancellation_policy")
        #col("summary"),
        #col("description"),
        #col("space")
    )

def build_hosts(df_json):
    return df_json.select(
        col("host.host_id"),
        col("host.host_name"),
        col("host.host_is_superhost").cast("int").alias("host_is_superhost"),
        col("host.host_response_time"),
        col("host.host_response_rate.$numberInt").cast("int").alias("host_response_rate"),
        col("host.host_location"),
        col("host.host_listings_count.$numberInt").cast("int").alias("host_listings_count"),
        col("host.host_total_listings_count.$numberInt").cast("int").alias("host_total_listings_count"),
        col("host.host_identity_verified").cast("int").alias("host_identity_verified")
    )

def build_listing_address(df_json):
    return df_json.select(
        col("_id").alias("listing_id"),
        col("address.country"),
        col("address.country_code"),
        col("address.market"),
        col("address.street"),
        col("address.suburb"),
        element_at(col("address.location.coordinates"), 1)["$numberDouble"].cast("double").alias("latitude"),
        element_at(col("address.location.coordinates"), 2)["$numberDouble"].cast("double").alias("longitude")
    )

def build_listing_amenities(df_json):
    return df_json.select(
        col("_id").alias("listing_id"),
        explode(col("amenities")).alias("amenity")
    )

def build_reviews(df_json):
    return df_json.select(
        explode(col("reviews")).alias("review")
    ).select(
        col("review._id").alias("review_id"),
        col("review.listing_id"),
        col("review.reviewer_id"),
        col("review.reviewer_name"),
        #col("review.comments"),
        (col("review.date.$date.$numberLong") / 1000).cast("timestamp").alias("review_date")
    )

def build_review_scores(df_json):
    return df_json.select(
        col("_id").alias("listing_id"),
        col("review_scores.review_scores_rating.$numberInt").cast("int").alias("rating"),
        col("review_scores.review_scores_cleanliness.$numberInt").cast("int").alias("cleanliness"),
        col("review_scores.review_scores_communication.$numberInt").cast("int").alias("communication"),
        col("review_scores.review_scores_location.$numberInt").cast("int").alias("location"),
        col("review_scores.review_scores_value.$numberInt").cast("int").alias("value")
    )

# ====================================================================
# GOLD LAYER 
# ====================================================================

# 1. Dimension: Listings (Descriptive)
def build_dim_listings(df_json):
    return df_json.select(
        col("_id").alias("listing_id"),
        col("name"),
        col("property_type"),
        col("room_type"),
        col("bed_type"),
        col("cancellation_policy")
    )

# 2. Dimension: Hosts (Unique hosts only)
def build_dim_hosts(df_json):
    return df_json.select(
        col("host.host_id"),
        col("host.host_name"),
        col("host.host_is_superhost").cast("int").alias("host_is_superhost"),
        col("host.host_location"),
        col("host.host_identity_verified").cast("int").alias("host_identity_verified")
    ).dropDuplicates(["host_id"])

# 3. Dimension: Locations
def build_dim_locations(df_json):
    return df_json.select(
        col("_id").alias("listing_id"),
        col("address.street"),
        col("address.suburb"),
        col("address.market"),
        col("address.country"),
        element_at(col("address.location.coordinates"), 1)["$numberDouble"].cast("double").alias("latitude"),
        element_at(col("address.location.coordinates"), 2)["$numberDouble"].cast("double").alias("longitude")
    )

# 4. Fact Table: Listings (The numerical core)
def build_fact_listings(df_json):
    return df_json.select(
        col("_id").alias("listing_id"),
        col("host.host_id"),
        col("accommodates.$numberInt").cast("int").alias("accommodates"),
        col("bedrooms.$numberInt").cast("int").alias("bedrooms"),
        col("beds.$numberInt").cast("int").alias("beds"),
        col("bathrooms.$numberDecimal").cast("double").alias("bathrooms"),
        col("price.$numberDecimal").cast("double").alias("price"),
        col("cleaning_fee.$numberDecimal").cast("double").alias("cleaning_fee"),
        col("security_deposit.$numberDecimal").cast("double").alias("security_deposit"),
        col("minimum_nights").cast("int"),
        col("maximum_nights").cast("int"),
        # Nested Review Scores
        col("review_scores.review_scores_rating.$numberInt").cast("int").alias("overall_rating"),
        col("review_scores.review_scores_cleanliness.$numberInt").cast("int").alias("cleanliness_score"),
        col("review_scores.review_scores_location.$numberInt").cast("int").alias("location_score"),
        col("review_scores.review_scores_value.$numberInt").cast("int").alias("value_score")
    )

# 5. Dimension: Amenities (Exploded)
def build_dim_amenities(df_json):
    return df_json.select(
        col("_id").alias("listing_id"),
        explode(col("amenities")).alias("amenity")
    )

# 6. Fact: Reviews
def build_fact_reviews(df_json):
    return df_json.select(
        explode(col("reviews")).alias("review")
    ).select(
        col("review._id").alias("review_id"),
        col("review.listing_id"),
        col("review.reviewer_id"),
        col("review.reviewer_name"),
        #col("review.comments"),
        (col("review.date.$date.$numberLong") / 1000).cast("timestamp").alias("review_date")
    )