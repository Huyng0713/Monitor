# Nginx Monitor

Ứng dụng theo dõi access log Nginx bằng FastAPI + SQLite, hiển thị dashboard thống kê và hỗ trợ mở rộng thêm nguồn log hoặc loại thống kê mới.

## Tính năng

- Dashboard thống kê request, IP, URL, mã trạng thái và bất thường.
- Tách logging theo 3 nhóm:
  - `logs/app.log`: trạng thái hoạt động
  - `logs/error.log`: lỗi xử lý và traceback
  - `logs/file.log`: lỗi đọc file và sự kiện liên quan file
- Thiết kế mở rộng:
  - thêm nguồn log mới qua `log_sources.py`
  - thêm thống kê mới qua `stats_service.py`
- Quản lý source code bằng Git.

## Cấu trúc chính

- `main.py`: điểm vào để chạy API.
- `routes.py`: định nghĩa HTTP API.
- `db.py`: kết nối và thao tác SQLite.
- `log.py`: cấu hình logging tập trung.
- `log_parse.py`: parser cho access log.
- `log_sources.py`: abstraction cho nguồn log.
- `stats_service.py`: lớp xử lý thống kê, tách khỏi HTTP layer.
- `frontend/index.html`: dashboard.

## Yêu cầu

- Python 3.11+

## Cài đặt

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install fastapi uvicorn
```

## Chạy ứng dụng

```bash
python3 main.py
```

Ứng dụng mặc định chạy tại [http://localhost:8000](http://localhost:8000).

## Nạp dữ liệu mẫu từ access log

```bash
python3 test.py
```

`test.py` sẽ đọc `access.log`, parse dữ liệu và insert vào `monitor.db`.

## Logging

Các file log được tạo trong thư mục `logs/`:

- `logs/app.log`: log trạng thái hoạt động như startup và request thành công.
- `logs/error.log`: log lỗi xử lý với traceback.
- `logs/file.log`: log lỗi đọc file hoặc parse file.

Có thể đổi mức log hoạt động bằng biến môi trường:

```bash
LOG_LEVEL=INFO python3 main.py
```

## Mở rộng nguồn log

Thêm class mới theo interface của `LogSource` trong `log_sources.py`.

Ví dụ:

```python
class ApiLogSource:
    name = "remote-api"

    def read_entries(self):
        ...
```

Sau đó truyền source mới vào luồng import dữ liệu.

## Mở rộng thống kê

Thêm method mới trong `StatsService` tại `stats_service.py`, rồi gọi method đó từ `routes.py`.

Ví dụ phù hợp:

- thống kê theo user-agent
- thống kê theo referer
- top IP theo khoảng thời gian

## Git workflow đề xuất

```bash
git status
git add .
git commit -m "refactor: add extensible logging and stats architecture"
```
