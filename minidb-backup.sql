--
-- PostgreSQL database dump
--

-- Dumped from database version 12.3 (Debian 12.3-1.pgdg100+1)
-- Dumped by pg_dump version 12.3 (Ubuntu 12.3-1.pgdg20.04+1)

-- Started on 2020-06-16 10:01:00 CEST

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 205 (class 1259 OID 16398)
-- Name: events; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.events (
    id integer NOT NULL,
    title text NOT NULL,
    part_of integer
);


ALTER TABLE public.events OWNER TO postgres;

--
-- TOC entry 204 (class 1259 OID 16396)
-- Name: events_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.events_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.events_id_seq OWNER TO postgres;

--
-- TOC entry 2927 (class 0 OID 0)
-- Dependencies: 204
-- Name: events_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.events_id_seq OWNED BY public.events.id;


--
-- TOC entry 203 (class 1259 OID 16387)
-- Name: series; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.series (
    id integer NOT NULL,
    name text NOT NULL,
    description text
);


ALTER TABLE public.series OWNER TO postgres;

--
-- TOC entry 202 (class 1259 OID 16385)
-- Name: series_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.series_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.series_id_seq OWNER TO postgres;

--
-- TOC entry 2928 (class 0 OID 0)
-- Dependencies: 202
-- Name: series_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.series_id_seq OWNED BY public.series.id;


--
-- TOC entry 2786 (class 2604 OID 16401)
-- Name: events id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.events ALTER COLUMN id SET DEFAULT nextval('public.events_id_seq'::regclass);


--
-- TOC entry 2785 (class 2604 OID 16390)
-- Name: series id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.series ALTER COLUMN id SET DEFAULT nextval('public.series_id_seq'::regclass);


--
-- TOC entry 2921 (class 0 OID 16398)
-- Dependencies: 205
-- Data for Name: events; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.events (id, title, part_of) FROM stdin;
0	Das Halteproblem	0
1	WebAssembly	1
2	Sortieren	0
3	Weihnachtsvorlesung	\N
4	Keller	0
\.


--
-- TOC entry 2919 (class 0 OID 16387)
-- Dependencies: 203
-- Data for Name: series; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.series (id, name, description) FROM stdin;
0	Informatik A	Einführung in Algorithmen und Datenstrukturen 
1	Web Technologien	Das gute Zeuch!
2	Lineare Algebra	\N
\.


--
-- TOC entry 2929 (class 0 OID 0)
-- Dependencies: 204
-- Name: events_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.events_id_seq', 1, false);


--
-- TOC entry 2930 (class 0 OID 0)
-- Dependencies: 202
-- Name: series_id_seq; Type: SEQUENCE SET; Schema: public; Owner: postgres
--

SELECT pg_catalog.setval('public.series_id_seq', 1, true);


--
-- TOC entry 2790 (class 2606 OID 16406)
-- Name: events events_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.events
    ADD CONSTRAINT events_pkey PRIMARY KEY (id);


--
-- TOC entry 2788 (class 2606 OID 16395)
-- Name: series series_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.series
    ADD CONSTRAINT series_pkey PRIMARY KEY (id);


--
-- TOC entry 2791 (class 2606 OID 16407)
-- Name: events part_of; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.events
    ADD CONSTRAINT part_of FOREIGN KEY (part_of) REFERENCES public.series(id);


-- Completed on 2020-06-16 10:01:00 CEST

--
-- PostgreSQL database dump complete
--

