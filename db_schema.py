-- Allegro Poland Demand Sensing Database Schema
-- PostgreSQL / SQLite compatible

CREATE TABLE IF NOT EXISTS platforms (
    platform_id INTEGER PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS hashtags (
    hashtag_id INTEGER PRIMARY KEY AUTOINCREMENT,
    tag VARCHAR(100) NOT NULL UNIQUE,
    category VARCHAR(50) DEFAULT 'desk_setup',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS products (
    product_id INTEGER PRIMARY KEY AUTOINCREMENT,
    keyword VARCHAR(200) NOT NULL UNIQUE,
    category VARCHAR(50) DEFAULT 'desk_setup',
    market VARCHAR(10) DEFAULT 'PL',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS social_metrics (
    metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
    date DATE NOT NULL,
    platform_id INTEGER NOT NULL,
    hashtag_id INTEGER NOT NULL,
    views BIGINT,
    videos INTEGER,
    likes BIGINT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (platform_id) REFERENCES platforms(platform_id),
    FOREIGN KEY (hashtag_id) REFERENCES hashtags(hashtag_id),
    UNIQUE(date, platform_id, hashtag_id)
);

CREATE TABLE IF NOT EXISTS marketplace_metrics (
    metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
    date DATE NOT NULL,
    product_id INTEGER NOT NULL,
    avg_price DECIMAL(10,2),
    offer_count INTEGER,
    sales_proxy INTEGER,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    UNIQUE(date, product_id)
);

-- Indexes for query performance
CREATE INDEX IF NOT EXISTS idx_social_date ON social_metrics(date);
CREATE INDEX IF NOT EXISTS idx_social_platform ON social_metrics(platform_id);
CREATE INDEX IF NOT EXISTS idx_social_hashtag ON social_metrics(hashtag_id);
CREATE INDEX IF NOT EXISTS idx_marketplace_date ON marketplace_metrics(date);
CREATE INDEX IF NOT EXISTS idx_marketplace_product ON marketplace_metrics(product_id);

-- Insert seed data
INSERT OR IGNORE INTO platforms (platform_id, name) VALUES 
    (1, 'TikTok'),
    (2, 'Instagram'),
    (3, 'Allegro');

-- Insert hashtags
INSERT OR IGNORE INTO hashtags (tag, category) VALUES
    ('desksetup', 'desk_setup'),
    ('homeoffice', 'desk_setup'),
    ('desksetupinspo', 'desk_setup'),
    ('studiosetup', 'desk_setup'),
    ('workspaceaesthetic', 'desk_setup'),
    ('gadgetsdesk', 'desk_setup'),
    ('workspaceinspo', 'desk_setup'),
    ('homeofficeideas', 'desk_setup');

-- Insert products
INSERT OR IGNORE INTO products (keyword, category, market) VALUES
    ('monitor light bar', 'desk_setup', 'PL'),
    ('rgb desk light', 'desk_setup', 'PL'),
    ('headphone stand', 'desk_setup', 'PL'),
    ('desk shelf', 'desk_setup', 'PL'),
    ('cable organizer', 'desk_setup', 'PL'),
    ('magnetic phone stand', 'desk_setup', 'PL'),
    ('LED clock', 'desk_setup', 'PL'),
    ('desk mat', 'desk_setup', 'PL');