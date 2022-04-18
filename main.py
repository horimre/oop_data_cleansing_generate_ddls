from productsCsv import ProductsCsv
from wishlistCsv import WishlistCsv
from contactsCsv import ContactsCsv

csv1 = ProductsCsv('product_task01_20220101.csv', '20220101').etl()
csv2 = ProductsCsv('product_task01_20220102.csv', '20220102').etl()
csv3 = ProductsCsv('product_task01_20220103.csv', '20220103').etl()

csv4 = WishlistCsv('wishlist_task01.csv').etl()
csv5 = ContactsCsv('contacts_task01.csv').etl()