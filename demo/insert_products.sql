-- insert_products.sql
--
-- Synthetic multilingual product catalog for the walkrie e-commerce
-- semantic search demo. Titles and descriptions are Japanese or Turkish,
-- randomly assembled from templates below — no real product/vendor data
-- of any kind. This demo exists to exercise jina-embeddings-v3 + its
-- LoRA task adapters (see config_product_demo.toml / config_product_query.toml)
-- on languages other than English.
--
-- Run against the demo database (see demo/README_ecommerce.md for full setup):
--   psql -d ecommerce_demo -f demo/insert_products.sql

CREATE TABLE IF NOT EXISTS products (
    id          bigserial PRIMARY KEY,
    sku         text NOT NULL,
    category    text NOT NULL,
    language    text NOT NULL,   -- 'ja' or 'tr'
    price       numeric(10,2) NOT NULL,
    currency    text NOT NULL,
    title       text NOT NULL,
    description text NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);

WITH archetypes(idx, language, category, name_tmpl, desc_tmpl, price_low, price_high, currency) AS (
    VALUES
    (1, 'ja', 'Electronics',
        '__ADJ__ワイヤレスイヤホン',
        '__ADJ__ワイヤレスイヤホン。ノイズキャンセリング機能を搭載し、__COLOR__カラーが人気です。Bluetooth 5.3対応で通勤や運動時にも快適にお使いいただけます。',
        3000, 25000, 'JPY'),
    (1, 'tr', 'Electronics',
        '__ADJ__ Kablosuz Kulaklık',
        '__ADJ__ kablosuz kulaklık. Aktif gürültü önleme özelliği ve __COLOR__ renk seçeneğiyle dikkat çekiyor. Bluetooth 5.3 desteğiyle spor ve günlük kullanım için idealdir.',
        800, 4500, 'TRY'),

    (2, 'ja', 'Home & Kitchen',
        '__ADJ__全自動コーヒーメーカー',
        '__ADJ__全自動コーヒーメーカー。__MATERIAL__デザインで、豆から挽きたての一杯を毎朝楽しめます。保温機能付きで忙しい朝にも便利です。',
        8000, 60000, 'JPY'),
    (2, 'tr', 'Home & Kitchen',
        '__ADJ__ Tam Otomatik Kahve Makinesi',
        '__ADJ__ tam otomatik kahve makinesi. __MATERIAL__ tasarımı sayesinde her sabah taze çekilmiş kahve keyfi sunar. Sıcak tutma özelliğiyle yoğun sabahlarda pratiktir.',
        1500, 9000, 'TRY'),

    (3, 'ja', 'Sports',
        '__ADJ__ランニングシューズ',
        '__ADJ__ランニングシューズ。軽量な__MATERIAL__素材を使用し、長距離走でも疲れにくいクッション性を実現しました。__COLOR__カラー展開。',
        4000, 18000, 'JPY'),
    (3, 'tr', 'Sports',
        '__ADJ__ Koşu Ayakkabısı',
        '__ADJ__ koşu ayakkabısı. Hafif __MATERIAL__ malzemeden üretilmiştir, uzun mesafelerde bile ayakları yormayan bir yastıklama sunar. __COLOR__ renk seçeneğiyle satışa sunulmaktadır.',
        900, 3500, 'TRY'),

    (4, 'ja', 'Fashion',
        '__ADJ__レザーハンドバッグ',
        '__ADJ__本革ハンドバッグ。__COLOR__のカラーが上品な印象を与え、普段使いからフォーマルな場まで幅広く活躍します。',
        6000, 40000, 'JPY'),
    (4, 'tr', 'Fashion',
        '__ADJ__ Deri El Çantası',
        '__ADJ__ hakiki deri el çantası. __COLOR__ rengi şık bir görünüm sunar; günlük kullanımdan resmi davetlere kadar geniş bir kullanım alanı sağlar.',
        1200, 7000, 'TRY'),

    (5, 'ja', 'Beauty',
        '__ADJ__美容液',
        '__ADJ__美容液。ヒアルロン酸とビタミンCを配合し、乾燥肌にもうるおいを与えます。__MATERIAL__使用感が特徴です。',
        2500, 12000, 'JPY'),
    (5, 'tr', 'Beauty',
        '__ADJ__ Yüz Serumu',
        '__ADJ__ yüz serumu. Hyalüronik asit ve C vitamini içeriğiyle kuru ciltlere bile nem kazandırır. __MATERIAL__ bir kullanım hissi sunar.',
        400, 2200, 'TRY'),

    (6, 'ja', 'Home & Kitchen',
        '__ADJ__LEDデスクランプ',
        '__ADJ__LEDデスクランプ。__COLOR__カラーで、目に優しい調光機能と三段階の明るさ調整が可能です。',
        2000, 9000, 'JPY'),
    (6, 'tr', 'Home & Kitchen',
        '__ADJ__ LED Masa Lambası',
        '__ADJ__ LED masa lambası. __COLOR__ renk seçeneğiyle göz yormayan, üç kademeli parlaklık ayarı sunar.',
        350, 1800, 'TRY'),

    (7, 'ja', 'Fashion',
        '__ADJ__バックパック',
        '__ADJ__バックパック。防水__MATERIAL__素材を採用し、ノートパソコン収納ポケット付きで通勤・通学に最適です。',
        3500, 15000, 'JPY'),
    (7, 'tr', 'Fashion',
        '__ADJ__ Sırt Çantası',
        '__ADJ__ sırt çantası. Su geçirmez __MATERIAL__ kumaştan üretilmiştir; dizüstü bilgisayar bölmesi sayesinde işe ve okula gitmek için idealdir.',
        600, 3200, 'TRY'),

    (8, 'ja', 'Toys & Games',
        '__ADJ__ボードゲーム',
        '__ADJ__ボードゲーム。家族や友人と楽しめる戦略ゲームで、__COLOR__パッケージが目印です。',
        2000, 8000, 'JPY'),
    (8, 'tr', 'Toys & Games',
        '__ADJ__ Kutu Oyunu',
        '__ADJ__ kutu oyunu. Aile ve arkadaşlarla oynanabilen bu strateji oyunu __COLOR__ kutu tasarımıyla dikkat çeker.',
        350, 1600, 'TRY')
),
picks AS (
    SELECT
        s AS n,
        (ARRAY['ja','tr'])[1 + floor(random() * 2)::int]   AS language,
        (1 + floor(random() * 8)::int)                     AS archetype_idx,
        floor(random() * 6)::int                           AS adj_idx,
        floor(random() * 5)::int                           AS color_idx,
        floor(random() * 5)::int                           AS material_idx
    FROM generate_series(1, 240) AS s
),
joined AS (
    SELECT
        p.n,
        a.category,
        a.name_tmpl,
        a.desc_tmpl,
        a.price_low,
        a.price_high,
        a.currency,
        CASE a.language
            WHEN 'ja' THEN (ARRAY['高性能な','プレミアムな','コンパクトな','人気の','新登場の','ベストセラーの'])[p.adj_idx + 1]
            ELSE           (ARRAY['yüksek performanslı','premium','kompakt','popüler','yeni çıkan','en çok satan'])[p.adj_idx + 1]
        END AS adj,
        CASE a.language
            WHEN 'ja' THEN (ARRAY['ブラック','ホワイト','ネイビー','ベージュ','レッド'])[p.color_idx + 1]
            ELSE           (ARRAY['siyah','beyaz','lacivert','bej','kırmızı'])[p.color_idx + 1]
        END AS color,
        CASE a.language
            WHEN 'ja' THEN (ARRAY['高品質な','耐久性のある','軽量な','プレミアムレザーの','防水'])[p.material_idx + 1]
            ELSE           (ARRAY['yüksek kaliteli','dayanıklı','hafif','premium deri','su geçirmez'])[p.material_idx + 1]
        END AS material,
        a.language
    FROM picks p
    JOIN archetypes a ON a.idx = p.archetype_idx AND a.language = p.language
)
INSERT INTO products (sku, category, language, price, currency, title, description)
SELECT
    'SKU-' || lpad(n::text, 6, '0'),
    category,
    language,
    round((price_low + random() * (price_high - price_low))::numeric, 2),
    currency,
    replace(name_tmpl, '__ADJ__', adj),
    replace(replace(replace(desc_tmpl, '__ADJ__', adj), '__COLOR__', color), '__MATERIAL__', material)
FROM joined;
