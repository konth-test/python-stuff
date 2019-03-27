import sqlite3
import csv
import json
import xml.etree.ElementTree as xml
import os
import pika
import re

# config
QUEUE_NAME =            "test"
QUEUE_URL =             "localhost"

OUTPUT_DIR =            "output"
OUTPUT_DIR_COUNTRIES =  os.path.join(OUTPUT_DIR, "countries")
NUM_INV_FILE =          os.path.join(OUTPUT_DIR, "num_inv.csv")
NUM_ITEMS_FILE =        os.path.join(OUTPUT_DIR, "num_items.csv")
ALBUMS_FILE =           os.path.join(OUTPUT_DIR_COUNTRIES, "{0}_albums.json")
BEST_SELLING_FILE =     os.path.join(OUTPUT_DIR_COUNTRIES, "{0}_best_selling_{1}_and_up.xml")

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


def init_csv_file(file_path, columns):
    try:
        fh = open(file_path, 'r')
        fh.close()
    except FileNotFoundError:
        with open(file_path, mode='w+', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(columns)


def init_all_files():
    if not os.path.exists(OUTPUT_DIR_COUNTRIES):
        os.makedirs(OUTPUT_DIR_COUNTRIES)

    init_csv_file(NUM_INV_FILE, NUM_INV_CSV_COLUMNS)
    init_csv_file(NUM_ITEMS_FILE, NUM_ITEMS_CSV_COLUMNS)


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


def db_country_exists(country):
    data = db_conn.execute("SELECT * FROM invoices WHERE BillingCountry = ? LIMIT 1", [country])

    return not (data.fetchone() is None)


def db_records_exists_for_year(year):
    year = str(year) + "-01-01"
    data = db_conn.execute("SELECT * FROM invoices WHERE InvoiceDate >= ? LIMIT 1", [year])

    return not (data.fetchone() is None)


def db_read_num_invoices(country):
    data = db_conn.execute("SELECT COUNT(*) FROM invoices WHERE BillingCountry = ?", [country])
    return data.fetchone()[0]


def db_read_num_items(country):
    data = db_conn.execute("""  SELECT
                                    COUNT(*)
                                FROM invoice_items
                                    LEFT JOIN invoices
                                        ON invoice_items.InvoiceId = invoices.InvoiceId
                                WHERE invoices.BillingCountry = ?""", [country])
    return data.fetchone()[0]


def db_read_purchased_albums(country):
    data = db_conn.execute("""  SELECT
                                    albums.Title
                                FROM albums
                                    LEFT JOIN tracks
                                        ON tracks.AlbumId = albums.AlbumId
                                    LEFT JOIN invoice_items
                                        ON invoice_items.TrackId = tracks.TrackId
                                    LEFT JOIN invoices
                                        ON invoice_items.InvoiceId = invoices.InvoiceId
                                WHERE invoices.BillingCountry = ?
                                GROUP BY ( albums.Title )""", [country])
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
                                        ON
                                        genres.GenreId = tracks.GenreId
                                WHERE
                                    invoices.BillingCountry = ?
                                    AND invoices.InvoiceDate >= ?
                                    AND genres.name = ?
                                GROUP BY invoice_items.TrackId
                                ORDER BY amount DESC
                                LIMIT 1""", [country, year, genre])

    return data.fetchone()


def output_to_csv(file_path, data):
    new_file = None
    mode = 'a'

    with open(file_path, mode='r', newline='') as file:
        file_str = file.read()
        res = re.search(data[0] + ",.*", file_str)
        if res:
            start = res.span()[0]
            end = res.span()[1]
            new_file = file_str[:start] + data[0] + "," + str(data[1]) + file_str[end:]
            mode = 'w+'

    with open(file_path, mode=mode, newline='') as file:
        if new_file:
            file.write(new_file)
        else:
            writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            writer.writerow(data)


def output_to_json(country, data):
    file_path = ALBUMS_FILE.format(country)

    with open(file_path, mode='w+') as file:
        json.dump(data, file, ensure_ascii=False)


def parse_album_data(data):
    new_data = {
        "name": data[0],
        "country": data[1],
        "amount": str(data[2]),
        "year": data[3]
    }

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


def output_to_db(country, num_inv, num_items, album_data):
    db_cursor.execute("REPLACE INTO num_invoices VALUES( ?, ? )", [country, num_inv])

    db_cursor.execute("REPLACE INTO num_items VALUES( ?, ? )", [country, num_items])

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
    if not db_country_exists(country):
        print("Invalid country name:", country)
        return

    if not db_records_exists_for_year(year):
        print("No records for or after year:", year)
        return

    num_inv = db_read_num_invoices(country)
    output_to_csv(NUM_INV_FILE, [country, num_inv])

    num_items = db_read_num_items(country)
    output_to_csv(NUM_ITEMS_FILE, [country, num_items])

    albums = db_read_purchased_albums(country)
    output_to_json(country, albums)

    best_selling = db_read_best_selling_track(country, year)
    if best_selling: # not always best selling will be found with Genre rock
        best_selling = parse_album_data(best_selling)
        output_to_xml( best_selling, year)

    output_to_db(country, num_inv, num_items, best_selling)


def initialize_module():
    init_all_files()
    init_mq()


initialize_module()
