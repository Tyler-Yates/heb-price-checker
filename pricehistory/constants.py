MIN_SLEEP_SECONDS = 1
MAX_SLEEP_SECONDS = 8

RECENCY_FILE_NAME = "recency.pickle"
RECENCY_MINIMUM_AGE_HOURS = 12
RECENCY_CATEGORY_COMPLETE = "*_*SKIP*_*"

CATEGORIES_QUERY = """
    query {
        shopNavigation {
            id
            displayName
        }
    }
"""

# Need to replace strings:
# - Category ID
# - Store ID
# - "after" cursor (for pagination)
PRODUCTS_QUERY = """
    query {
        browseCategory(
            categoryId: "%s"
            storeId: %s
            shoppingContext: CURBSIDE_PICKUP
            limit: 100
            cursor: %s
        ) {
            pageTitle
            records {
                id
                displayName

                productImageUrls {
                    size
                    url
                }

                bestAvailable
                onAd
                isNew

                isComboLoco
                deal
                pricedByWeight
                brand {
                    name
                    isOwnBrand
                }
                SKUs {
                    id
                    contextPrices {
                        context
                        isOnSale
                        unitListPrice {
                            unit
                            formattedAmount
                        }
                        priceType
                        listPrice {
                            unit
                            formattedAmount
                        }
                        salePrice {
                            formattedAmount
                        }
                    }
                    productAvailability
                    customerFriendlySize
                    skuPrice {
                        listPrice {
                            displayName
                        }
                    }
                }
            }
            total
            hasMoreRecords
            nextCursor
            previousCursor
        }
    }
"""

# Need to replace strings:
# - username
# - password
# - host
DB_CONNECTION_STRING = "mongodb+srv://%s:%s@%s/?retryWrites=true&w=majority"
