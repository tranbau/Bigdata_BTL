# Bài tập lớn bigdata
Lưu trữ và xử lí dữ liệu chứng khoán
### Yêu Cầu
#### Docker
#### NodeJS
#### Python

### Cài Đặt
1. **Clone dự án từ repository:**
    ```sh
    git clone https://github.com/yourusername/yourproject.git](https://github.com/tranbau/Bigdata_BTL.git
    ```
2. **Cài đặt các dependencies cho phần backend NodeJS**
    ```sh
    cd .\stream\visualize\BE\
    npm install
    ```
3. **Cài đặt Live Server trong VSC cho phần Frontend**

## Sử dụng
1. **Khởi chạy docker**
    ```sh
    docker-compose up -d
    ```
2. **Setup các config cần thiết sau khi chạy các container**
    ```sh
    .\bin\setup.bat 
    ```
3. **Tạo postgresql table theo commands/pos.txt**
    ```sh
    docker exec -it postgres psql -U user -W stock 
    ```
    ```sh
    CREATE TABLE ..
    ```
4. **Chạy kafka consumer**
    ```sh
    cd .\batch\kafka\
    python .\consumer1.py 
    ```
5. **Chạy kafka provider**
    ```sh
    cd .\batch\kafka\
    python .\provider.py 
    ```
6. **Chạy kafka provider**
    ```sh
    cd .\batch\kafka\
    python .\provider.py 
    ```
7. **Truy cập Zeppline**
    ```
    http://localhost:8082
    Import file .zpln trong folder zeppline-code 
    ```
8. **Spark streaming**
    ```
    Thực hiện theo commands\spark.txt
    ```  



