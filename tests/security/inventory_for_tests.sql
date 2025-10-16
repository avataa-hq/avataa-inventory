--
-- TOC entry 3415 (class 0 OID 16392)
-- Dependencies: 215
-- Data for Name: tmo; Type: TABLE DATA; Schema: public; Owner: inventory_admin
--

INSERT INTO tmo ("primary", p_id, name, icon, description, virtual, global_uniqueness, id, version, latitude, longitude, status, created_by, modified_by, creation_date, modification_date, inherit_location) VALUES ('[]', NULL, 'LVL1', NULL, NULL, false, true, 1, 1, NULL, NULL, NULL, '', '', '2023-06-14 11:49:36.918245', '2023-06-14 11:49:36.918245', false);
INSERT INTO tmo ("primary", p_id, name, icon, description, virtual, global_uniqueness, id, version, latitude, longitude, status, created_by, modified_by, creation_date, modification_date, inherit_location) VALUES ('[]', 1, 'LVL2', NULL, NULL, false, true, 2, 2, NULL, NULL, NULL, '', '', '2023-06-14 11:50:04.216789', '2023-06-14 11:52:56.161535', false);
INSERT INTO tmo ("primary", p_id, name, icon, description, virtual, global_uniqueness, id, version, latitude, longitude, status, created_by, modified_by, creation_date, modification_date, inherit_location) VALUES ('[]', 2, 'LVL3', NULL, NULL, false, true, 3, 1, NULL, NULL, NULL, '', '', '2023-06-14 11:50:57.785266', '2023-06-14 11:50:57.785266', false);


--
-- TOC entry 3421 (class 0 OID 16449)
-- Dependencies: 221
-- Data for Name: tprm; Type: TABLE DATA; Schema: public; Owner: inventory_admin
--

INSERT INTO tprm (tmo_id, name, description, val_type, multiple, required,  returnable,  "constraint", prm_link_filter, "group", id, version, created_by, modified_by, creation_date, modification_date) VALUES (1, 'obj1.str', 'obj1.str', 'str', false, false, false, false, false, NULL, NULL, NULL, 1, 1, '', '', '2023-06-14 11:54:24.068599', '2023-06-14 11:54:24.068599');
INSERT INTO tprm (tmo_id, name, description, val_type, multiple, required,  returnable,  "constraint", prm_link_filter, "group", id, version, created_by, modified_by, creation_date, modification_date) VALUES (2, 'obj2.prm_link', 'obj2.prm_link', 'prm_link', false, false, false, false, false, '1', NULL, NULL, 2, 1, '', '', '2023-06-14 11:56:49.525526', '2023-06-14 11:56:49.525526');
INSERT INTO tprm (tmo_id, name, description, val_type, multiple, required,  returnable,  "constraint", prm_link_filter, "group", id, version, created_by, modified_by, creation_date, modification_date) VALUES (2, 'obj2.str', 'obj2.str', 'str', false, false, false, false, false, NULL, NULL, NULL, 3, 1, '', '', '2023-06-14 11:57:50.901278', '2023-06-14 11:57:50.901278');
INSERT INTO tprm (tmo_id, name, description, val_type, multiple, required,  returnable,  "constraint", prm_link_filter, "group", id, version, created_by, modified_by, creation_date, modification_date) VALUES (2, 'obj3.mo_link', 'obj3.mo_link', 'mo_link', false, false, false, false, false, '2', NULL, NULL, 4, 1, '', '', '2023-06-14 12:00:21.5582', '2023-06-14 12:00:21.5582');
INSERT INTO tprm (tmo_id, name, description, val_type, multiple, required,  returnable,  "constraint", prm_link_filter, "group", id, version, created_by, modified_by, creation_date, modification_date) VALUES (3, 'obj4.str', 'obj4.str', 'str', false, false, false, false, false, NULL, NULL, NULL, 5, 1, '', '', '2023-06-14 12:02:23.882571', '2023-06-14 12:02:23.882571');
INSERT INTO tprm (tmo_id, name, description, val_type, multiple, required,  returnable,  "constraint", prm_link_filter, "group", id, version, created_by, modified_by, creation_date, modification_date) VALUES (3, 'obj4.prm_link', 'obj4.prm_link', 'prm_link', false, false, false, false, false, '3', NULL, NULL, 6, 1, '', '', '2023-06-14 12:08:02.808424', '2023-06-14 12:08:02.808424');
INSERT INTO tprm (tmo_id, name, description, val_type, multiple, required,  returnable,  "constraint", prm_link_filter, "group", id, version, created_by, modified_by, creation_date, modification_date) VALUES (3, 'obj5.prm_link', 'obj5.prm_link', 'prm_link', false, false, false, false, false, '1', NULL, NULL, 7, 1, '', '', '2023-06-14 12:08:45.484832', '2023-06-14 12:08:45.484832');


--
-- TOC entry 3419 (class 0 OID 16419)
-- Dependencies: 219
-- Data for Name: mo; Type: TABLE DATA; Schema: public; Owner: inventory_admin
--

INSERT INTO mo (pov, geometry, tmo_id, p_id, point_a_id, point_b_id, model, id, version, name, active, latitude, longitude, status, creation_date, modification_date) VALUES ('null', 'null', 1, NULL, NULL, NULL, NULL, 1, 1, '1', true, NULL, NULL, NULL, '2024-02-05 03:14:15.926535', '2024-02-05 03:14:15.926535');
INSERT INTO mo (pov, geometry, tmo_id, p_id, point_a_id, point_b_id, model, id, version, name, active, latitude, longitude, status, creation_date, modification_date) VALUES ('null', 'null', 2, 1, NULL, NULL, NULL, 2, 1, '2', true, NULL, NULL, NULL, '2024-02-05 03:14:15.926536', '2024-02-05 03:14:15.926536');
INSERT INTO mo (pov, geometry, tmo_id, p_id, point_a_id, point_b_id, model, id, version, name, active, latitude, longitude, status, creation_date, modification_date) VALUES ('null', 'null', 2, 1, NULL, NULL, NULL, 3, 1, '3', true, NULL, NULL, NULL, '2024-02-05 03:14:15.926537', '2024-02-05 03:14:15.926537');
INSERT INTO mo (pov, geometry, tmo_id, p_id, point_a_id, point_b_id, model, id, version, name, active, latitude, longitude, status, creation_date, modification_date) VALUES ('null', 'null', 3, 3, NULL, NULL, NULL, 4, 1, '4', true, NULL, NULL, NULL, '2024-02-05 03:14:15.926538', '2024-02-05 03:14:15.926538');
INSERT INTO mo (pov, geometry, tmo_id, p_id, point_a_id, point_b_id, model, id, version, name, active, latitude, longitude, status, creation_date, modification_date) VALUES ('null', 'null', 3, 3, NULL, NULL, NULL, 5, 1, '5', true, NULL, NULL, NULL, '2024-02-05 03:14:15.926539', '2024-02-05 03:14:15.926539');


--
-- TOC entry 3423 (class 0 OID 16466)
-- Dependencies: 223
-- Data for Name: prm; Type: TABLE DATA; Schema: public; Owner: inventory_admin
--

INSERT INTO prm (tprm_id, mo_id, value, id, version) VALUES (1, 1, 'obj1.val.str', 1, 1);
INSERT INTO prm (tprm_id, mo_id, value, id, version) VALUES (3, 2, 'obj2.val.str', 2, 1);
INSERT INTO prm (tprm_id, mo_id, value, id, version) VALUES (2, 2, '1', 3, 1);
INSERT INTO prm (tprm_id, mo_id, value, id, version) VALUES (3, 3, 'obj3.val.str', 4, 1);
INSERT INTO prm (tprm_id, mo_id, value, id, version) VALUES (4, 3, '2', 5, 1);
INSERT INTO prm (tprm_id, mo_id, value, id, version) VALUES (5, 4, 'obj4.val.str', 6, 1);
INSERT INTO prm (tprm_id, mo_id, value, id, version) VALUES (6, 4, '4', 7, 1);
INSERT INTO prm (tprm_id, mo_id, value, id, version) VALUES (5, 5, 'obj5.val.str', 8, 1);
INSERT INTO prm (tprm_id, mo_id, value, id, version) VALUES (7, 5, '1', 9, 1);


--
-- TOC entry 3427 (class 0 OID 16511)
-- Dependencies: 227
-- Data for Name: mo_permission; Type: TABLE DATA; Schema: public; Owner: inventory_admin
--

INSERT INTO mo_permission (id, permission, permission_name, "create", read, update, delete, admin, root_permission_id, parent_id) VALUES (1, 'default', 'default', true, true, true, true, false, NULL, 4);
INSERT INTO mo_permission (id, permission, permission_name, "create", read, update, delete, admin, root_permission_id, parent_id) VALUES (2, 'default', 'default', false, true, false, false, false, 1, 3);
INSERT INTO mo_permission (id, permission, permission_name, "create", read, update, delete, admin, root_permission_id, parent_id) VALUES (3, 'default', 'default', false, true, false, false, false, 1, 1);


--
-- TOC entry 3425 (class 0 OID 16488)
-- Dependencies: 225
-- Data for Name: tmo_permission; Type: TABLE DATA; Schema: public; Owner: inventory_admin
--

INSERT INTO tmo_permission (id, permission, permission_name, "create", read, update, delete, admin, root_permission_id, parent_id) VALUES (1, 'realm_access.__reader', 'reader', true, true, true, true, false, NULL, 3);
INSERT INTO tmo_permission (id, permission, permission_name, "create", read, update, delete, admin, root_permission_id, parent_id) VALUES (2, 'realm_access.__reader', 'reader', true, true, true, true, true, 1, 2);
INSERT INTO tmo_permission (id, permission, permission_name, "create", read, update, delete, admin, root_permission_id, parent_id) VALUES (3, 'default', 'default', false, true, false, false, false, 1, 1);


--
-- TOC entry 3429 (class 0 OID 16534)
-- Dependencies: 229
-- Data for Name: tprm_permission; Type: TABLE DATA; Schema: public; Owner: inventory_admin
--

INSERT INTO tprm_permission (id, permission, permission_name, "create", read, update, delete, admin, root_permission_id, parent_id) VALUES (1, 'default', 'default', true, true, true, true, false, NULL, 5);
INSERT INTO tprm_permission (id, permission, permission_name, "create", read, update, delete, admin, root_permission_id, parent_id) VALUES (2, 'default', 'default', true, true, true, true, false, NULL, 6);
INSERT INTO tprm_permission (id, permission, permission_name, "create", read, update, delete, admin, root_permission_id, parent_id) VALUES (3, 'default', 'default', true, true, true, true, false, NULL, 7);


-- INSERT INTO events (id, event_type, model_id, "user", event_time, event) VALUES (1, 'TMOCreate', 1, '', '2023-06-14 11:49:36.936371', '{"TMO": {"id": 1, "icon": null, "name": "LVL1", "p_id": null, "status": null, "primary": [], "version": 1, "virtual": false, "latitude": null, "longitude": null, "created_by": "", "description": null, "modified_by": "", "creation_date": "2023-06-14T11:49:36.918245", "global_uniqueness": true, "modification_date": "2023-06-14T11:49:36.918245"}}');
-- INSERT INTO events (id, event_type, model_id, "user", event_time, event) VALUES (2, 'TMOCreate', 2, '', '2023-06-14 11:50:04.242431', '{"TMO": {"id": 2, "icon": null, "name": "LVL2", "p_id": null, "status": null, "primary": [], "version": 1, "virtual": false, "latitude": null, "longitude": null, "created_by": "", "description": null, "modified_by": "", "creation_date": "2023-06-14T11:50:04.216789", "global_uniqueness": true, "modification_date": "2023-06-14T11:50:04.216789"}}');
-- INSERT INTO events (id, event_type, model_id, "user", event_time, event) VALUES (3, 'TMOCreate', 3, '', '2023-06-14 11:50:57.795221', '{"TMO": {"id": 3, "icon": null, "name": "LVL3", "p_id": 2, "status": null, "primary": [], "version": 1, "virtual": false, "latitude": null, "longitude": null, "created_by": "", "description": null, "modified_by": "", "creation_date": "2023-06-14T11:50:57.785266", "global_uniqueness": true, "modification_date": "2023-06-14T11:50:57.785266"}}');
-- INSERT INTO events (id, event_type, model_id, "user", event_time, event) VALUES (4, 'TMOUpdate', 2, '', '2023-06-14 11:52:56.182497', '{"TMO": {"id": 2, "icon": null, "name": "LVL2", "p_id": 1, "status": null, "primary": [], "version": 2, "virtual": false, "latitude": null, "longitude": null, "created_by": "", "description": null, "modified_by": "", "creation_date": "2023-06-14T11:50:04.216789", "global_uniqueness": true, "modification_date": "2023-06-14T11:52:56.161535"}}');
-- INSERT INTO events (id, event_type, model_id, "user", event_time, event) VALUES (5, 'TPRMCreate', 1, '', '2023-06-14 11:54:24.079738', '{"TPRM": {"id": 1, "name": "obj1.str", "group": null, "tmo_id": 1, "version": 1, "multiple": false, "required": false, "val_type": "str", "automation": false, "constraint": null, "created_by": "", "returnable": false, "searchable": false, "description": "obj1.str", "modified_by": "", "creation_date": "2023-06-14T11:54:24.068599", "prm_link_filter": null, "modification_date": "2023-06-14T11:54:24.068599"}}');
-- INSERT INTO events (id, event_type, model_id, "user", event_time, event) VALUES (6, 'TPRMCreate', 2, '', '2023-06-14 11:56:49.542017', '{"TPRM": {"id": 2, "name": "obj2.prm_link", "group": null, "tmo_id": 2, "version": 1, "multiple": false, "required": false, "val_type": "prm_link", "automation": false, "constraint": "1", "created_by": "", "returnable": false, "searchable": false, "description": "obj2.prm_link", "modified_by": "", "creation_date": "2023-06-14T11:56:49.525526", "prm_link_filter": null, "modification_date": "2023-06-14T11:56:49.525526"}}');
-- INSERT INTO events (id, event_type, model_id, "user", event_time, event) VALUES (7, 'TPRMCreate', 3, '', '2023-06-14 11:57:50.920595', '{"TPRM": {"id": 3, "name": "obj2.str", "group": null, "tmo_id": 2, "version": 1, "multiple": false, "required": false, "val_type": "str", "automation": false, "constraint": null, "created_by": "", "returnable": false, "searchable": false, "description": "obj2.str", "modified_by": "", "creation_date": "2023-06-14T11:57:50.901278", "prm_link_filter": null, "modification_date": "2023-06-14T11:57:50.901278"}}');
-- INSERT INTO events (id, event_type, model_id, "user", event_time, event) VALUES (8, 'TPRMCreate', 4, '', '2023-06-14 12:00:21.565791', '{"TPRM": {"id": 4, "name": "obj3.mo_link", "group": null, "tmo_id": 2, "version": 1, "multiple": false, "required": false, "val_type": "mo_link", "automation": false, "constraint": "2", "created_by": "", "returnable": false, "searchable": false, "description": "obj3.mo_link", "modified_by": "", "creation_date": "2023-06-14T12:00:21.558200", "prm_link_filter": null, "modification_date": "2023-06-14T12:00:21.558200"}}');
-- INSERT INTO events (id, event_type, model_id, "user", event_time, event) VALUES (9, 'TPRMCreate', 5, '', '2023-06-14 12:02:23.901074', '{"TPRM": {"id": 5, "name": "obj4.str", "group": null, "tmo_id": 3, "version": 1, "multiple": false, "required": false, "val_type": "str", "automation": false, "constraint": null, "created_by": "", "returnable": false, "searchable": false, "description": "obj4.str", "modified_by": "", "creation_date": "2023-06-14T12:02:23.882571", "prm_link_filter": null, "modification_date": "2023-06-14T12:02:23.882571"}}');
-- INSERT INTO events (id, event_type, model_id, "user", event_time, event) VALUES (10, 'TPRMCreate', 6, '', '2023-06-14 12:08:02.840786', '{"TPRM": {"id": 6, "name": "obj4.prm_link", "group": null, "tmo_id": 3, "version": 1, "multiple": false, "required": false, "val_type": "prm_link", "automation": false, "constraint": "3", "created_by": "", "returnable": false, "searchable": false, "description": "obj4.prm_link", "modified_by": "", "creation_date": "2023-06-14T12:08:02.808424", "prm_link_filter": null, "modification_date": "2023-06-14T12:08:02.808424"}}');
-- INSERT INTO events (id, event_type, model_id, "user", event_time, event) VALUES (11, 'TPRMCreate', 7, '', '2023-06-14 12:08:45.507875', '{"TPRM": {"id": 7, "name": "obj5.prm_link", "group": null, "tmo_id": 3, "version": 1, "multiple": false, "required": false, "val_type": "prm_link", "automation": false, "constraint": "1", "created_by": "", "returnable": false, "searchable": false, "description": "obj5.prm_link", "modified_by": "", "creation_date": "2023-06-14T12:08:45.484832", "prm_link_filter": null, "modification_date": "2023-06-14T12:08:45.484832"}}');
-- INSERT INTO events (id, event_type, model_id, "user", event_time, event) VALUES (12, 'PRMCreate', 1, '', '2023-06-14 12:11:01.307921', '{"PRM": {"id": 1, "mo_id": 1, "value": "obj1.val.str", "tprm_id": 1, "version": 1}}');
-- INSERT INTO events (id, event_type, model_id, "user", event_time, event) VALUES (13, 'MOCreate', 1, '', '2023-06-14 12:11:01.32299', '{"MO": {"id": 1, "pov": null, "name": 1, "p_id": null, "model": null, "active": true, "status": null, "tmo_id": 1, "version": 1, "geometry": null, "latitude": null, "longitude": null, "point_a_id": null, "point_b_id": null}}');
-- INSERT INTO events (id, event_type, model_id, "user", event_time, event) VALUES (14, 'PRMCreate', 2, '', '2023-06-14 12:13:58.622747', '{"PRM": {"id": 2, "mo_id": 2, "value": "obj2.val.str", "tprm_id": 3, "version": 1}}');
-- INSERT INTO events (id, event_type, model_id, "user", event_time, event) VALUES (15, 'PRMCreate', 3, '', '2023-06-14 12:13:58.640231', '{"PRM": {"id": 3, "mo_id": 2, "value": 1, "tprm_id": 2, "version": 1}}');
-- INSERT INTO events (id, event_type, model_id, "user", event_time, event) VALUES (16, 'MOCreate', 2, '', '2023-06-14 12:13:58.650288', '{"MO": {"id": 2, "pov": null, "name": 2, "p_id": 1, "model": null, "active": true, "status": null, "tmo_id": 2, "version": 1, "geometry": null, "latitude": null, "longitude": null, "point_a_id": null, "point_b_id": null}}');
-- INSERT INTO events (id, event_type, model_id, "user", event_time, event) VALUES (17, 'PRMCreate', 4, '', '2023-06-14 12:25:31.459182', '{"PRM": {"id": 4, "mo_id": 3, "value": "obj3.val.str", "tprm_id": 3, "version": 1}}');
-- INSERT INTO events (id, event_type, model_id, "user", event_time, event) VALUES (18, 'PRMCreate', 5, '', '2023-06-14 12:25:31.476919', '{"PRM": {"id": 5, "mo_id": 3, "value": 2, "tprm_id": 4, "version": 1}}');
-- INSERT INTO events (id, event_type, model_id, "user", event_time, event) VALUES (19, 'MOCreate', 3, '', '2023-06-14 12:25:31.487812', '{"MO": {"id": 3, "pov": null, "name": 3, "p_id": 1, "model": null, "active": true, "status": null, "tmo_id": 2, "version": 1, "geometry": null, "latitude": null, "longitude": null, "point_a_id": null, "point_b_id": null}}');
-- INSERT INTO events (id, event_type, model_id, "user", event_time, event) VALUES (20, 'PRMCreate', 6, '', '2023-06-14 12:27:31.827057', '{"PRM": {"id": 6, "mo_id": 4, "value": "obj4.val.str", "tprm_id": 5, "version": 1}}');
-- INSERT INTO events (id, event_type, model_id, "user", event_time, event) VALUES (21, 'PRMCreate', 7, '', '2023-06-14 12:27:31.846689', '{"PRM": {"id": 7, "mo_id": 4, "value": 4, "tprm_id": 6, "version": 1}}');
-- INSERT INTO events (id, event_type, model_id, "user", event_time, event) VALUES (22, 'MOCreate', 4, '', '2023-06-14 12:27:31.858367', '{"MO": {"id": 4, "pov": null, "name": 4, "p_id": 3, "model": null, "active": true, "status": null, "tmo_id": 3, "version": 1, "geometry": null, "latitude": null, "longitude": null, "point_a_id": null, "point_b_id": null}}');
-- INSERT INTO events (id, event_type, model_id, "user", event_time, event) VALUES (23, 'PRMCreate', 8, '', '2023-06-14 12:29:27.045553', '{"PRM": {"id": 8, "mo_id": 5, "value": "obj5.val.str", "tprm_id": 5, "version": 1}}');
-- INSERT INTO events (id, event_type, model_id, "user", event_time, event) VALUES (24, 'PRMCreate', 9, '', '2023-06-14 12:29:27.067196', '{"PRM": {"id": 9, "mo_id": 5, "value": 1, "tprm_id": 7, "version": 1}}');
-- INSERT INTO events (id, event_type, model_id, "user", event_time, event) VALUES (25, 'MOCreate', 5, '', '2023-06-14 12:29:27.080026', '{"MO": {"id": 5, "pov": null, "name": 5, "p_id": 3, "model": null, "active": true, "status": null, "tmo_id": 3, "version": 1, "geometry": null, "latitude": null, "longitude": null, "point_a_id": null, "point_b_id": null}}');


--
-- TOC entry 3443 (class 0 OID 0)
-- Dependencies: 216
-- Name: events_id_seq; Type: SEQUENCE SET; Schema: public; Owner: inventory_admin
--

SELECT pg_catalog.setval('events_id_seq', 25, true);


--
-- TOC entry 3444 (class 0 OID 0)
-- Dependencies: 218
-- Name: mo_id_seq; Type: SEQUENCE SET; Schema: public; Owner: inventory_admin
--

SELECT pg_catalog.setval('mo_id_seq', 5, true);


--
-- TOC entry 3445 (class 0 OID 0)
-- Dependencies: 226
-- Name: mo_permission_id_seq; Type: SEQUENCE SET; Schema: public; Owner: inventory_admin
--

SELECT pg_catalog.setval('mo_permission_id_seq', 3, true);


--
-- TOC entry 3446 (class 0 OID 0)
-- Dependencies: 222
-- Name: prm_id_seq; Type: SEQUENCE SET; Schema: public; Owner: inventory_admin
--

SELECT pg_catalog.setval('prm_id_seq', 9, true);


--
-- TOC entry 3447 (class 0 OID 0)
-- Dependencies: 214
-- Name: tmo_id_seq; Type: SEQUENCE SET; Schema: public; Owner: inventory_admin
--

SELECT pg_catalog.setval('tmo_id_seq', 3, true);


--
-- TOC entry 3448 (class 0 OID 0)
-- Dependencies: 224
-- Name: tmo_permission_id_seq; Type: SEQUENCE SET; Schema: public; Owner: inventory_admin
--

SELECT pg_catalog.setval('tmo_permission_id_seq', 3, true);


--
-- TOC entry 3449 (class 0 OID 0)
-- Dependencies: 220
-- Name: tprm_id_seq; Type: SEQUENCE SET; Schema: public; Owner: inventory_admin
--

SELECT pg_catalog.setval('tprm_id_seq', 7, true);


--
-- TOC entry 3450 (class 0 OID 0)
-- Dependencies: 228
-- Name: tprm_permission_id_seq; Type: SEQUENCE SET; Schema: public; Owner: inventory_admin
--

SELECT pg_catalog.setval('tprm_permission_id_seq', 3, true);
