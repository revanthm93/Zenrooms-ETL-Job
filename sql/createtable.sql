CREATE TABLE public.tweets
(
    tweetid bigint,
    text text COLLATE pg_catalog."default",
    contributors_enabled boolean,
    created_at text COLLATE pg_catalog."default",
    default_profile boolean,
    default_profile_image boolean,
    favourites_count bigint,
    follow_request_sent boolean,
    followers_count bigint,
    following boolean,
    friends_count bigint,
    geo_enabled boolean,
    has_extended_profile boolean,
    id bigint,
    id_str text COLLATE pg_catalog."default",
    is_translation_enabled boolean,
    is_translator boolean,
    lang text COLLATE pg_catalog."default",
    listed_count bigint,
    location text COLLATE pg_catalog."default",
    name text COLLATE pg_catalog."default",
    notifications boolean,
    protected boolean,
    screen_name text COLLATE pg_catalog."default",
    statuses_count bigint,
    time_zone text COLLATE pg_catalog."default",
    translator_type text COLLATE pg_catalog."default",
    url text COLLATE pg_catalog."default",
    utc_offset text COLLATE pg_catalog."default",
    verified boolean
)

TABLESPACE pg_default;

ALTER TABLE public.tweets
    OWNER to postgres;