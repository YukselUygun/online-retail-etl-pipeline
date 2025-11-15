# Online Retail ETL Pipeline

Modern Data Engineering Best Practices ile geliştirilmiş uçtan uca bir ETL pipeline.

Bu proje, Online Retail dataset'i üzerinde çalışan ve gerçek şirket seviyesinde tasarlanan bir ETL (Extract–Transform–Load) pipeline uygulamasıdır.

## Özellikler

- Python tabanlı ETL script
- Config management (`config.yaml`)
- Logging sistemi (`logs/etl.log`)
- Exception handling
- Data Quality Checks
- Staging & Clean katmanları
- PostgreSQL veri ambarı
- ETL performans ölçümü
- Terminal ETL raporu
- Üretim (prod) standartlarına göre tasarlanmış mimari

## Proje Yapısı

```
online-retail-etl-pipeline/
├── config.yaml                 # Yapılandırma dosyası
├── main.py                     # Ana ETL script
├── requirements.txt            # Python bağımlılıkları
├── README.md                   # Proje dokümantasyonu
├── data/
│   ├── raw/                    # Ham veri
│   ├── staging/                # İşleme aşaması
│   └── clean/                  # Temizlenmiş veri
├── logs/
│   └── etl.log                 # ETL log dosyası
├── src/
│   ├── __init__.py
│   ├── extractor.py            # Veri çıkarma
│   ├── transformer.py          # Veri dönüştürme
│   ├── loader.py               # Veri yükleme
│   ├── validator.py            # Veri kalite kontrolleri
│   └── utils.py                # Yardımcı fonksiyonlar
└── tests/
    └── test_etl.py             # Unit testler
```

## Kurulum

1. Gerekli paketleri yükleyin:

```bash
pip install -r requirements.txt
```

2. `config.yaml` dosyasını konfigüre edin:

```yaml
database:
  host: localhost
  port: 5432
  name: online_retail
  user: postgres
```

3. Veritabanı tablolarını oluşturun:

```bash
psql -U postgres -d online_retail -f sql/create_tables.sql
```

## Kullanım

ETL pipeline'ı çalıştırmak için:

```bash
python main.py
```

## Veri Kaynakları

- `data/online_retail.csv`: Ham Online Retail dataset'i

## Lisans

MIT License

## Mimari (Architecture Diagram)

```
    +--------------------------+
    |        CSV Source        |
    +------------+-------------+
             |
             v
    +--------------------------+
    |      Extract (Python)    |
    +------------+-------------+
             |
             v
    +--------------------------+
    |  Transform (Cleaning)    |
    |  - Price hesaplama       |
    |  - İptal flag            |
    |  - Customer validation   |
    |  - Null & quality checks |
    +------------+-------------+
             |
             v
    +--------------------------+
    |   Load → Staging Table   |
    +------------+-------------+
             |
             v
    +--------------------------+
    |     Clean Layer Model    |
    +--------------------------+
```

## ETL Raporu Örneği

```
ETL Report:
-----------
Total Rows Loaded: 540455
Clean Rows:       530693
Cancelled Ratio:  1.72%
Valid Customers:  75.28%
ETL Duration:     34.07 seconds
Status:           SUCCESS
```
