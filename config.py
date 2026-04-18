REDSHIFT_URL = (
    "jdbc:redshift://bostatask.970769152894.us-east-1."
    "redshift-serverless.amazonaws.com:5439/dev"
)

REDSHIFT_PROPERTIES = {
    "user": "serverlessuser",
    "password": "BostaTest1234",   
    "driver": "com.amazon.redshift.jdbc42.Driver",
    "ssl": "true",
    "sslfactory": "com.amazon.redshift.ssl.NonValidatingFactory"
}

JSON_PATH = "D:/Bosta/Data/listingsAndReviews.json"