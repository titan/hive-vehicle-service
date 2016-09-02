
CREATE TABLE vehicle_model (
  vehicle_code char(32) PRIMARY KEY,
  vin_code char(17) NOT NULL,
  vehicle_name char(128) NOT NULL,
  brand_name char(128) NOT NULL,
  family_name char(128) NOT NULL,
  body_type char(128),
  engine_number char(128),
  engine_desc char(1024) NOT NULL,
  gearbox_name char(128),
  year_pattern char(128),
  group_name char(128) NOT NULL,
  cfg_level char(128),
  purchase_price real NOT NULL,
  purchase_price_tax real NOT NULL,
  seat integer,
  effluent_standard char(128),
  pl char(128) NOT NULL,
  fuel_jet_type char(128),
  driven_type char(128)
);

CREATE TABLE person(
    id uuid PRIMARY KEY,
    name char(128) NOT NULL,
    identity_no char(18) NOT NULL,
    phone char(11) NOT NULL,
    identity_frontal_view char(128),
    identity_rear_view char(128),
    license_frontal_view char(128),
    license_rear_view char(128)
);
CREATE TABLE vehicle(
    id uuid PRIMARY KEY,
    user_id uuid NOT NULL,
    owner uuid NOT NULL,
    vehicle_code char(32) NOT NULL,
    license_no char(128),
    engine_no char(128) NOT NULL,
    register_date date,
    average_mileage char(128) NOT NULL,
    is_transfer boolean NOT NULL,
    receipt_no char(128),
    receipt_date date,
    last_insurance_company char(128),
    insurance_due_date date,
    driving_frontal_view char(128),
    driving_rear_view char(128),
    FOREIGN KEY (owner) REFERENCES person(id),
    FOREIGN KEY (vehicle_code) REFERENCES vehicle_model(vehicle_code)
);
CREATE TABLE drivers(
    id uuid PRIMARY KEY,
    vid uuid NOT NULL,
    pid uuid NOT NULL,
    is_primary boolean NOT NULL,
    FOREIGN KEY (vid) REFERENCES vehicle(id),
    FOREIGN KEY (pid) REFERENCES person(id)
); 
