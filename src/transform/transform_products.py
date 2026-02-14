from ..extract.extract import extract_collection

data_products = extract_collection('products')



# ---------- tirar dados faltantes

# data_products = data_products.dropna(subset=['title', 'price'])

# ----------- tratar dados duplicados

# unique_fields = ['title', 'sku']
# data_products = data_products.drop_duplicates(subset=unique_fields, keep='first')

# tratar title description category brand sku warrantyInformation shippingInformation availabilityStatus returnPolicy thumbnail

# data_products['title'] = data_products['title'].str.strip()
# data_products['description'] = data_products['description'].str.strip()
# data_products['category'] = data_products['category'].str.strip()
# data_products['brand'] = data_products['brand'].str.strip()
# data_products['sku'] = data_products['sku'].str.strip()
# data_products['warrantyInformation'] = data_products['warrantyInformation'].str.strip()
# data_products['shippingInformation'] = data_products['shippingInformation'].str.strip()
# data_products['availabilityStatus'] = data_products['availabilityStatus'].str.strip()
# data_products['returnPolicy'] = data_products['returnPolicy'].str.strip()
# data_products['thumbnail'] = data_products['thumbnail'].str.strip()

# tratamento price

# data_products = data_products[data_products['price'] >= 0]

# tratamento discount %

# data_products = data_products[(data_products['discountPercentage'] >=0) & (data_products['discountPercentage'] <= 100)]

# tratamento rating

# data_products = data_products[(data_products['rating'] >=0) & (data_products['rating'] <= 5)]

# tratamento stock

# data_products = data_products[data_products['stock'] >=0]

# tratamento weight

# data_products = data_products[data_products['weight'] >=0]

# tratamento minimumOrderQuantity

# data_products = data_products[data_products['minimumOrderQuantity'] >=0]

