-- This script was generated by a beta version of the ERD tool in pgAdmin 4.
-- Please log an issue at https://redmine.postgresql.org/projects/pgadmin4/issues/new if you find any bugs, including reproduction steps.
BEGIN;

CREATE TABLE IF NOT EXISTS public.artists_song
(
    song_id character varying(30) NOT NULL,
    artist_id character varying(30) NOT NULL,
    unique_id character varying(60) NOT NULL,
    index serial NOT NULL,
    PRIMARY KEY (unique_id)
);

CREATE TABLE IF NOT EXISTS public.spotify_albuns
(
    album_id character varying(30) NOT NULL,
    album_name text NOT NULL,
    album_release_date date NOT NULL,
    album_total_tracks smallint NOT NULL,
    album_uri text NOT NULL,
    album_url text NOT NULL,
    album_cover_url text NOT NULL,
    artist_id character varying(30) NOT NULL,
    date_time_inserted timestamp without time zone,
    PRIMARY KEY (album_id)
);

CREATE TABLE IF NOT EXISTS public.spotify_artists
(
    artist_id character varying(30) NOT NULL,
    artist_name text NOT NULL,
    artist_uri text NOT NULL,
    artist_url text NOT NULL,
    date_time_inserted timestamp without time zone,
    PRIMARY KEY (artist_id)
);

CREATE TABLE IF NOT EXISTS public.spotify_tracks
(
    song_id character varying(30) NOT NULL,
    song_name text NOT NULL,
    song_duration integer NOT NULL,
    song_popularity smallint NOT NULL,
    song_explicit boolean NOT NULL,
    song_url text NOT NULL,
    played_at character varying(25) NOT NULL,
    date date NOT NULL,
    album_id character varying(30) NOT NULL,
    date_time_inserted timestamp without time zone,
    PRIMARY KEY (played_at)
);

CREATE TABLE IF NOT EXISTS public.spotify_tracks_artists_song
(
    spotify_tracks_song_id character varying(30) NOT NULL,
    artists_song_song_id character varying(30) NOT NULL
);

CREATE TABLE IF NOT EXISTS public.spotify_artists_artists_song
(
    spotify_artists_artist_id character varying(30) NOT NULL,
    artists_song_artist_id character varying(30) NOT NULL
);

ALTER TABLE public.spotify_tracks_artists_song
    ADD FOREIGN KEY (spotify_tracks_song_id)
    REFERENCES public.spotify_tracks (song_id)
    NOT VALID;


ALTER TABLE public.spotify_tracks_artists_song
    ADD FOREIGN KEY (artists_song_song_id)
    REFERENCES public.artists_song (song_id)
    NOT VALID;


ALTER TABLE public.spotify_artists_artists_song
    ADD FOREIGN KEY (spotify_artists_artist_id)
    REFERENCES public.spotify_artists (artist_id)
    NOT VALID;


ALTER TABLE public.spotify_artists_artists_song
    ADD FOREIGN KEY (artists_song_artist_id)
    REFERENCES public.artists_song (artist_id)
    NOT VALID;


ALTER TABLE public.spotify_tracks
    ADD FOREIGN KEY (artist_id)
    REFERENCES public.spotify_artists (artist_id)
    NOT VALID;


ALTER TABLE public.spotify_tracks
    ADD FOREIGN KEY (album_id)
    REFERENCES public.spotify_albuns (album_id)
    NOT VALID;


ALTER TABLE public.spotify_tracks
    ADD FOREIGN KEY (artist_id)
    REFERENCES public.spotify_artists (artist_id)
    NOT VALID;

END;