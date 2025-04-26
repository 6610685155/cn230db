import requests
import sqlite3
import time

# ส่วนเอาข้อมูลเข้า database
"""
url = "https://api.jikan.moe/v4/anime?page=1"
animes = []                     # สร้าง list เก็บข้อมูล
pages_to_fetch = 40             # จำนวนหน้าที่ต้องการดึงข้อมูล

conn = sqlite3.connect('anime_data.db') #เชื่อมฐานข้อมูล (conn คือการเชื่อมต่อกับฐานข้อมูล )
cursor = conn.cursor()                  #conn.cursor() สร้าง cursor object ที่สามารถใช้คำสั่ง SQL กับฐานข้อมูลที่เชื่อมอยู่

#execute() ใช้ส่งคำสั่ง SQL ไปให้ฐานข้อมูลทำงาน
#สร้าง table anime (IF NOT EXISTS คือ ถ้าตารางยังไม่มีก็จะสร้างใหม่แต่ถ้ามีแล้วก็ไม่ทำอะไร)
cursor.execute(''' 
CREATE TABLE IF NOT EXISTS anime (
    id INTEGER PRIMARY KEY,
    title TEXT,
    type TEXT,
    source TEXT,
    episodes INTEGER,
    rating TEXT,
    popularity INTEGER,
    score REAL,
    genres TEXT,
    studios TEXT
)
''')

# loop ดึงข้อมูลจากหลายๆหน้า
# ผมหน่วงเวลาโดยใช้ time.sleep() เพื่อไม่ให้เกิด Error429 หรือ Too Many Requests เนื่องจากตัวเว็บกำหนด Rate Limiting ไว้ 
current_page = 1                                            # เริ่มต้นที่หน้า 1
while current_page <= pages_to_fetch:
    response = requests.get(f"{url}&page={current_page}")   # ขอข้อมูลแต่ละหน้าจากเว็บผ่าน api 
    if response.status_code == 200:                         # เช็คสถานะตอบกลับถ้า 200 แสดงว่าสำเร็จ
        data = response.json()                              # แปลงข้อมูลที่ได้เป็น JSON
        animes.extend(data['data'])                         # เพิ่มข้อมูลใหม่ที่ได้รับจาก API ลงใน list
        current_page += 1                                   # ไปยังหน้าถัดไป
        time.sleep(1)                                       # หน่วงเวลา 1 วินาที
    else:                                                   # ถ้าไม่ใช่ 200 เช่น error 404,500
        print("Error:", response.status_code)               #แสดง status error เช่น error 404
        break

# loop บันทึกข้อมูล
for anime in animes:
    id = anime['mal_id']                        # id ของ anime
    title = anime['title']                      # ชื่อ anime
    type_ = anime.get('type', None)             # ประเภท เช่น TV, Movie (ใช้ .get() เพื่อป้องกัน error ถ้าไม่มีข้อมูลจะได้ None แทน)
    source = anime.get('source', None)          # ที่มา เช่น Manga, Novel, Original
    episodes = anime.get('episodes', None)      # จำนวนตอน
    rating = anime.get('rating', None)          # เรทอายุ เช่น R-17+
    popularity = anime.get('popularity', None)  # ความนิยม     
    score = anime.get('score', None)            # คะแนนรีวิว

    # genres: รวมชื่อ genre ทั้งหมด เช่น "Action, Comedy"
    # ', '.join([...]) เชื่อมรวมกันเป็น String เดียว โดยคั่นด้วยเครื่องหมาย คอมม่า+ช่องว่าง (, ) 
    genres = ', '.join([genre['name'] for genre in anime.get('genres', [])])

    # studios: รวมชื่อสตูดิโอ เช่น "MAPPA, Madhouse"
    studios = ', '.join([studio['name'] for studio in anime.get('studios', [])])

    #ใส่ข้อมูลเข้า database
    # VALUES: เป็นคำสั่งที่ใช้ระบุค่าของข้อมูลที่ต้องการแทรกเข้าไปในคอลัมน์ที่กล่าวถึงตรงส่วน insert
    # ? เป็น placeholder คือเราจะใส่ค่าลงในตำแหน่งเหล่านี้ตามคอลัมน์ที่กล่าวถึงตรงส่วน insert
    cursor.execute('''
    INSERT OR IGNORE INTO anime (id, title, type, source, episodes, rating, popularity, score, genres, studios)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) 
    ''', (
        id,
        title,
        type_,
        source,
        episodes,
        rating,
        popularity,
        score,
        genres,
        studios
    ))

print("create database finished")
conn.commit()
"""
conn = sqlite3.connect('anime_data.db')
cursor = conn.cursor() 

#จำนวนข้อมูลใน database
cursor.execute("SELECT COUNT(*) FROM anime") 
row_count = cursor.fetchone()[0]  # ดึงค่าindexแรกจากผลลัพธ์ tuple
print(f"Total number of data = {row_count} rows")  # แสดงจำนวนข้อมูล

#---------------------------------------------------------------------------------------

#แสดงข้อมูลทั้งหมด
#for row in cursor.execute("SELECT * FROM anime"):
#    print(f"ID: {row[0]}, Title: {row[1]}, Type: {row[2]}, Source: {row[3]}, Episodes: {row[4]}, Rating: {row[5]}, Popularity: {row[6]}, Score: {row[7]}, Genres: {row[8]}, Studios: {row[9]}")

#ดูจำนวน type ทั้งหมด
print("\n-------------------------------------")
print("\nAll type")
cursor.execute('''
    SELECT DISTINCT type
    FROM anime;
''')
all_type = cursor.fetchall()
for anime in all_type:
    print(f"{anime[0]}")
#---------------------------------------------------------------------------------------

# 5 อันดับ anime ที่มี score สูงที่สุด
print("\n-------------------------------------")
print("\nMost 5 anime with highest score")
cursor.execute('''
    SELECT title, score
    FROM anime
    ORDER BY score DESC
    LIMIT 5;
''')
top_5_animes = cursor.fetchall()
for index,anime in enumerate(top_5_animes, 1): #ใช้ enumerate เพื่อใส่เลขอันดับตามจำนวนการวนซ้ำ โดยให้ index เริ่มที่ 1
    print(f"{index}. {anime[0]}, Score: {anime[1]}")
    index += 1

#---------------------------------------------------------------------------------------

# 10 อันดับ anime แนว Action และ fantasy ที่มีความนิยมสูงที่สุด
print("\n-------------------------------------")
print("\nMost 10 action and fantasy anime with highest popularity")
cursor.execute('''
    SELECT title, popularity, genres
    FROM anime
    WHERE genres LIKE '%Action%' AND genres LIKE '%Fantasy%'
    ORDER BY popularity DESC
    LIMIT 10;
''')
top_10_af_animes = cursor.fetchall()
for index,anime in enumerate(top_10_af_animes, 1): 
    print("")
    print(f"{index}. {anime[0]}\n   Popularity: {anime[1]}\n   Genres: {anime[2]}")
    index += 1

#---------------------------------------------------------------------------------------

# 3 อันดับแนว(genre) anime ที่เยอะที่สุด
print("\n-------------------------------------")
print("\nMost 3 genre in anime")
cursor.execute('SELECT genres FROM anime')
rows = cursor.fetchall()

genre_counts = {} # เก็บจำนวนของแต่ละ genre

for row in rows:
    genres = row[0]  # ข้อมูล genres ที่ดึงออกมา เช่น "Action, Adventure, Drama"
    if genres:  
        for genre in genres.split(', '): # แยก genres และนับจำนวนของแต่ละ genre
            if genre in genre_counts:
                genre_counts[genre] += 1  # ถ้ามี genre นี้อยู่แล้วให้เพิ่มค่าขึ้น
            else:
                genre_counts[genre] = 1  # ถ้ายังไม่มี genre นี้ให้เริ่มนับจาก 1

# genre_counts จะได้ผลประมาณนี้ {"Action": 120, "Fantasy": 90, ...}
# genre_counts.items() จะได้ผลประมาณนี้ [("Action", 120), ("Fantasy", 90), ...] จาก genre_counts
# key=lambda x: x[1] คือฟังก์ชันที่ใช้ในการเปรียบเทียบและจัดลำดับค่าในแต่ละ tuple ที่อยู่ในรูปแบบ (genre, count)
# key=lambda x: x[1] จะทำให้คำสั่ง sorted() จัดลำดับตามจำนวนของ genre
# reverse=True จัดเรียงจากมากไปหาน้อย reverse=False จัดเรียงจากน้อยไปหามาก sorted()[:3] เลือก 3 อันดับแรก
sorted_genres = sorted(genre_counts.items(), key=lambda x: x[1], reverse=True)[:3]

for index,(genre, count) in enumerate(sorted_genres, 1):
    print(f"{index}. {genre}, count = {count}")

#---------------------------------------------------------------------------------------

# 6 อันดับ anime ที่มีจำนวนตอนน้อยที่สุดที่ในประเภท OVA หรือ TV
print("\n-------------------------------------")
print("\nTop 6 Anime with the fewest episodes")
cursor.execute('''
    SELECT title, episodes
    FROM anime
    WHERE episodes IS NOT NULL
    AND type IN ('OVA', 'TV')
    ORDER BY episodes ASC
    LIMIT 6;
''')
top_6_le = cursor.fetchall()
for index, anime in enumerate(top_6_le, 1):
    print(f"{index}. {anime[0]}, episodes = {anime[1]}")