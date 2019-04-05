import sqlite3
import csv
import json
import xml.etree.ElementTree as xml
import os
import pika
#
# config
QUEUE_NAME =            "test"
QUEUE_URL =             "localhost"

OUTPUT_DIR =            "output"
OUTPUT_DIR_COUNTRIES =      os.path.join(OUTPUT_DIR, "countries")
OUTPUT_DIR_BEST_SELLING =   os.path.join(OUTPUT_DIR, "best_selling")
NUM_INV_FILE =              os.path.join(OUTPUT_DIR, "num_inv.csv")
NUM_ITEMS_FILE =            os.path.join(OUTPUT_DIR, "num_items.csv")
ALBUMS_FILE =               os.path.join(OUTPUT_DIR_COUNTRIES, "{0}_albums.json")
BEST_SELLING_FILE =         os.path.join(OUTPUT_DIR_BEST_SELLING, "{0}_best_selling_{1}_and_up.xml")

NUM_ITEMS_CSV_COLUMNS = ["Country", "Items amount"]
NUM_INV_CSV_COLUMNS = ["Country", "Invoices amount"]

db_conn = None
db_cursor = None

mq_conn = pika.BlockingConnection(pika.ConnectionParameters(QUEUE_URL))
mq_channel = mq_conn.channel()


def message_is_valid(body):
    if not body["db_path"] or not body["country"] or not body["year"]:
        return False

    if not db_is_valid(body["db_path"]):
        print("Invalid DB Path")
        return False

    if not isinstance(body["year"], int) or body["year"] < 0 or body["year"] > 9999:
        print("Invalid year")
        return False

    return True


def init_mq():
    mq_channel.queue_declare(queue=QUEUE_NAME)

    def callback(ch, method, properties, body):
        body = json.loads(body)
        print("---")
        print("received message: ", body)

        if not message_is_valid(body):
            print("Invalid message, skipping\n")
            return

        db_connect(body["db_path"])
        db_create_tables()
        process_message(body["country"], body["year"])
        db_close_connection()
        print()

    mq_channel.basic_consume(callback, queue=QUEUE_NAME, no_ack=True)

    print("Listening to queue...")
    mq_channel.start_consuming()

def create_dir_if_not_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)


def init_dirs():
    create_dir_if_not_exists(OUTPUT_DIR_COUNTRIES)
    create_dir_if_not_exists(OUTPUT_DIR_BEST_SELLING)


def db_is_valid(db_path):
    return os.path.isfile(db_path)


def db_connect(db_path):
    global db_conn, db_cursor

    db_conn = sqlite3.connect(db_path)
    db_cursor = db_conn.cursor()


def db_close_connection():
    db_cursor.close()
    db_conn.close()


def db_create_tables():
    db_conn.execute(""" CREATE TABLE IF NOT EXISTS
                            num_invoices(
                                countryName VARCHAR(255),
                                invSaleAmount INTEGER,
                                UNIQUE( countryName )
                            )""")

    db_conn.execute(""" CREATE TABLE IF NOT EXISTS
                            num_items(
                                countryName VARCHAR(255) UNIQUE,
                                ItemSaleAmount INTEGER,
                                UNIQUE( countryName )
                            )""")

    db_conn.execute(""" CREATE TABLE IF NOT EXISTS
                            best_selling(
                                 trackName VARCHAR(255),
                                 countryName VARCHAR(255),
                                 saleAmount INTEGER,
                                 year DATE,
                                 UNIQUE( countryName, year )
                             )""")


def db_read_num_invoices():
    data = db_conn.execute("""  SELECT
                                    BillingCountry,
                                    COUNT(*)
                                FROM invoices
                                GROUP BY BillingCountry""")
    return data.fetchall()


def db_read_num_items():
    data = db_conn.execute("""  SELECT
                                    BillingCountry,
                                    COUNT(*)
                                FROM invoice_items
                                    LEFT JOIN invoices
                                        ON invoice_items.InvoiceId = invoices.InvoiceId
                                GROUP BY BillingCountry""")
    return data.fetchall()


def db_read_purchased_albums():
    data = db_conn.execute("""  SELECT
                                    BillingCountry, albums.Title
                                FROM invoices
                                    LEFT JOIN invoice_items
                                        ON invoice_items.InvoiceId = invoices.InvoiceId
                                    LEFT JOIN tracks
                                        ON tracks.TrackId = invoice_items.TrackId
                                    LEFT JOIN albums
                                        ON albums.AlbumId = tracks.AlbumId
                                GROUP BY BillingCountry, Title""")
    return data.fetchall()


def db_read_best_selling_track(country, year):
    year = str(year) + "-01-01"
    genre = 'Rock'
    data = db_conn.execute("""  SELECT
                                    tracks.Name,
                                    invoices.BillingCountry,
                                    COUNT(invoice_items.TrackId ) as amount,
                                    strftime('%Y', invoices.InvoiceDate)
                                FROM invoice_items
                                    LEFT JOIN invoices
                                        ON invoice_items.InvoiceId = invoices.InvoiceId
                                    LEFT JOIN tracks
                                        ON tracks.TrackId = invoice_items.TrackId
                                    LEFT JOIN genres
                                        ON genres.GenreId = tracks.GenreId
                                WHERE
                                    invoices.BillingCountry = ?
                                    AND invoices.InvoiceDate >= ?
                                    AND genres.name = ?
                                GROUP BY invoice_items.TrackId
                                ORDER BY amount DESC
                                LIMIT 1""", [country, year, genre])

    return data.fetchone()


def output_to_csv(file_path, data, columns):
    with open(file_path, mode='w+', newline='') as file:
            writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            writer.writerow(columns)
            for row in data:
                writer.writerow(row)


def output_to_json(data):
    for country in data:
        c_data = data[country]
        file_path = ALBUMS_FILE.format(country)

        with open(file_path, mode='w+') as file:
            json.dump(c_data, file, ensure_ascii=False)


def parse_best_selling_data(data):
    new_data = {
        "name": data[0],
        "country": data[1],
        "amount": str(data[2]),
        "year": data[3]
    }

    return new_data

def parse_album_data(data):
    new_data = {}

    for country, album in data:
        if country in new_data:
            new_data[country].append(album)
        else:
            new_data[country] = []

    return new_data

def output_to_xml(data, query_year):
    track_xml_data = xml.Element('Track')
    track_xml_data.set("Name", data["name"])

    xml.SubElement(track_xml_data, 'Amount').text = data["amount"]
    xml.SubElement(track_xml_data, 'Country').text = data["country"]
    xml.SubElement(track_xml_data, 'Year').text = data["year"]

    file_name = BEST_SELLING_FILE.format(data["country"], query_year)

    tree = xml.ElementTree(track_xml_data)
    tree.write(file_name)


def output_to_db(invoice_data, items_data, album_data):
    for country, n in invoice_data:
        db_cursor.execute("REPLACE INTO num_invoices VALUES( ?, ? )", [country, n])

    for country, n in items_data:
        db_cursor.execute("REPLACE INTO num_items VALUES( ?, ? )", [country, n])

    if album_data:
        db_cursor.execute("REPLACE INTO best_selling VALUES( ?, ?, ?, ? )",
                          [
                              album_data["name"],
                              album_data["country"],
                              album_data["amount"],
                              album_data["year"]
                          ])

    db_conn.commit()


def process_message(country, year):
    invoice_data = db_read_num_invoices()
    output_to_csv(NUM_INV_FILE, invoice_data, NUM_INV_CSV_COLUMNS)

    item_data = db_read_num_items()
    output_to_csv(NUM_ITEMS_FILE, item_data, NUM_ITEMS_CSV_COLUMNS)

    album_data = parse_album_data( db_read_purchased_albums() )
    output_to_json(album_data)

    best_selling = db_read_best_selling_track(country, year)
    if best_selling: # not always best selling will be found with Genre rock
        best_selling = parse_best_selling_data(best_selling)
        output_to_xml( best_selling, year)
    else:
        print("No records found for best selling")

    output_to_db(invoice_data, item_data, best_selling)

def initialize_module():
    init_dirs()
    init_mq()


initialize_module()
