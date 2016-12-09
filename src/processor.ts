import { Processor, Config, ModuleFunction, DoneFunction, rpc, async_serial, async_serial_ignore } from "hive-processor";
import { Client as PGClient, QueryResult } from "pg";
import { RedisClient, Multi } from "redis";
import * as bunyan from "bunyan";
import { servermap, triggermap } from "hive-hostmap";
import * as uuid from "node-uuid";
import * as msgpack from "msgpack-lite";
import * as nanomsg from "nanomsg";
import * as http from "http";
import * as bluebird from "bluebird";

declare module "redis" {
  export interface RedisClient extends NodeJS.EventEmitter {
    hgetAsync(key: string, field: string): Promise<any>;
    hsetAsync(key: string, field: string, field2: string): Promise<any>;
    hincrbyAsync(key: string, field: string, value: number): Promise<any>;
    setexAsync(key: string, ttl: number, value: string): Promise<any>;
  }
  export interface Multi extends NodeJS.EventEmitter {
    execAsync(): Promise<any>;
  }
}


let log = bunyan.createLogger({
  name: "vehicle-processor",
  streams: [
    {
      level: "info",
      path: "/var/log/vehicle-processor-info.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1d",   // daily rotation
      count: 7        // keep 7 back copies
    },
    {
      level: "error",
      path: "/var/log/vehicle-processor-error.log",  // log ERROR and above to a file
      type: "rotating-file",
      period: "1w",   // daily rotation
      count: 3        // keep 3 back copies
    }
  ]
});

let config: Config = {
  dbhost: process.env["DB_HOST"],
  dbuser: process.env["DB_USER"],
  dbport: process.env["DB_PORT"],
  database: process.env["DB_NAME"],
  dbpasswd: process.env["DB_PASSWORD"],
  cachehost: process.env["CACHE_HOST"],
  addr: "ipc:///tmp/vehicle.ipc"
};

let processor = new Processor(config);
let vehicle_trigger = nanomsg.socket("pub");
vehicle_trigger.bind(triggermap.vehicle);

// 新车已上牌个人
processor.call("setVehicleOnCard", (db: PGClient, cache: RedisClient, done: DoneFunction, name: string, identity_no: string, phone: string, uid: string, recommend: string, vehicle_code: string, license_no: string, engine_no: string,
  register_date: any, average_mileage: string, is_transfer: boolean, last_insurance_company: string, insurance_due_date: any, fuel_type: string, vin: string, callback: string) => {
  log.info("setVehicleOnCard");
  (async () => {
    try {
      let pid = "";
      let owner = {};
      await db.query("BEGIN");
      const person = await db.query("SELECT id, name, identity_no, phone FROM person WHERE identity_no = $1 AND deleted = false", [identity_no]);
      log.info("old person: " + JSON.stringify(person.rows));
      if (person["rowCount"] !== 0) {
        pid = person.rows[0]["id"];
        owner = {
          id: pid,
          name: person.rows[0]["name"],
          identity_no: person.rows[0]["identity_no"],
          phone: person.rows[0]["phone"]
        };
      } else {
        pid = uuid.v1();
        log.info("new perosn" + pid);
        await db.query("INSERT INTO person (id, name, identity_no, phone) VALUES ($1, $2, $3, $4)", [pid, name, identity_no, phone]);
        owner = {
          id: pid,
          name: name,
          identity_no: identity_no,
          phone: phone
        };
      }
      let vids = await db.query("SELECT id FROM vehicles WHERE vin = $1", [vin]);
      let vid = "";
      if (vids["rowCount"] !== 0) {
        log.info("old vehicle id" + JSON.stringify(vids.rows));
        vid = vids.rows[0]["id"];
      } else {
        vid = uuid.v1();
        log.info("new vehicle id: " + vid);
        await db.query("INSERT INTO vehicles (id, user_id, owner, owner_type, recommend, vehicle_code,license_no,engine_no,register_date,average_mileage,is_transfer, last_insurance_company,insurance_due_date, fuel_type, vin) VALUES ($1, $2, $3, $4, $5, $6, $7, $8 ,$9, $10 ,$11, $12, $13, $14, $15)", [vid, uid, pid, 0, recommend, vehicle_code, license_no, engine_no, register_date, average_mileage, is_transfer, last_insurance_company, insurance_due_date, fuel_type, vin]);
        let vehicle = {
          id: vid,
          user_id: uid,
          owner: owner,
          owner_type: 0,
          recommend: recommend,
          drivers: [],
          vehicle_code: vehicle_code,
          vin_code: vin,
          license_no: license_no,
          engine_no: engine_no,
          register_date: register_date,
          average_mileage: average_mileage,
          is_transfer: is_transfer,
          last_insurance_company: last_insurance_company,
          insurance_due_date: insurance_due_date,
          fuel_type: fuel_type
        };
        const vehicle_model_json = await cache.hgetAsync("vehicle-model-entities", vehicle_code);
        vehicle["vehicle_model"] = JSON.parse(vehicle_model_json);
        let multi = bluebird.promisifyAll(cache.multi()) as Multi;
        multi.hset("vehicle-entities", vid, JSON.stringify(vehicle));
        multi.lpush("vehicle-" + uid, vid);
        multi.lpush("vehicle", vid);
        await multi.execAsync();
      }
      await db.query("COMMIT");
      await cache.setexAsync(callback, 30, JSON.stringify({ code: 200, data: vid }));
      done();
    } catch (e) {
      log.error(e);
      await db.query("ROLLBACK");
      await cache.setexAsync(callback, 30, JSON.stringify({ code: 500, msg: e.message }));
      done();
    }
  })();
});

// 新车未上牌个人
processor.call("setVehicle", (db: PGClient, cache: RedisClient, done: DoneFunction, name: string, identity_no: string, phone: string, uid: string, recommend: string, vehicle_code: string, engine_no: string, average_mileage: string, is_transfer: boolean, receipt_no: string, receipt_date: any, last_insurance_company: string, fuel_type: string, vin: string, callback: string) => {
  log.info("setVehicle");
  (async () => {
    try {
      let pid = "";
      let owner = {};
      await db.query("BEGIN");
      const person = await db.query("SELECT id, name, identity_no, phone FROM person WHERE identity_no = $1 AND deleted = false", [identity_no]);
      log.info("old person: " + JSON.stringify(person.rows));
      if (person["rowCount"] !== 0) {
        pid = person.rows[0]["id"];
        owner = {
          id: pid,
          name: person.rows[0]["name"],
          identity_no: person.rows[0]["identity_no"],
          phone: person.rows[0]["phone"]
        };
      } else {
        pid = uuid.v1();
        owner = {
          id: pid,
          name: name,
          identity_no: identity_no,
          phone: phone
        };
        log.info("new perosn" + pid);
        await db.query("INSERT INTO person (id, name, identity_no, phone) VALUES ($1, $2, $3, $4)", [pid, name, identity_no, phone]);
      }
      let vids = await db.query("SELECT id FROM vehicles WHERE vin = $1", [vin]);
      let vid = "";
      if (vids["rowCount"] !== 0) {
        log.info("old vehicle id" + JSON.stringify(vids.rows));
        vid = vids.rows[0]["id"];
      } else {
        vid = uuid.v1();
        log.info("new vehicle id: " + vid);
        await db.query("INSERT INTO vehicles (id, user_id, owner, owner_type, recommend, vehicle_code, engine_no,average_mileage,is_transfer,receipt_no, receipt_date,last_insurance_company, fuel_type, vin) VALUES ($1, $2, $3, $4, $5, $6, $7, $8 ,$9, $10, $11, $12, $13, $14)", [vid, uid, pid, 0, recommend, vehicle_code, engine_no, average_mileage, is_transfer, receipt_no, receipt_date, last_insurance_company, fuel_type, vin]);
        let vehicle = {
          id: vid,
          user_id: uid,
          owner: owner,
          owner_type: 0,
          recommend: recommend,
          drivers: [],
          vehicle_code: vehicle_code,
          vin_code: vin,
          engine_no: engine_no,
          license_no: null,
          average_mileage: average_mileage,
          is_transfer: is_transfer,
          receipt_no: receipt_no,
          receipt_date: receipt_date,
          last_insurance_company: last_insurance_company,
          fuel_type: fuel_type
        };
        const vehicle_model_json = await cache.hgetAsync("vehicle-model-entities", vehicle_code);
        vehicle["vehicle_model"] = JSON.parse(vehicle_model_json);
        let multi = bluebird.promisifyAll(cache.multi()) as Multi;
        multi.hset("vehicle-entities", vid, JSON.stringify(vehicle));
        multi.lpush("vehicle-" + uid, vid);
        multi.lpush("vehicle", vid);
        await multi.execAsync();
      }
      await db.query("COMMIT");
      await cache.setexAsync(callback, 30, JSON.stringify({ code: 200, data: vid }));
      done();
    } catch (e) {
      log.error(e);
      await db.query("ROLLBACK");
      await cache.setexAsync(callback, 30, JSON.stringify({ code: 500, msg: e.message }));
      done();
    }
  })();
});

interface InsertDriverCtx {
  pids: any;
  dids: any;
  vid: string;
  cache: RedisClient;
  db: PGClient;
  done: DoneFunction;
}

processor.call("setDriver", (db: PGClient, cache: RedisClient, done: DoneFunction, vid: string, drivers: any, callback: string) => {
  log.info("setDriver " + vid);
  (async () => {
    try {
      let pids = [];
      await db.query("BEGIN");
      let vehicleJson = await cache.hgetAsync("vehicle-entities", vid);
      let vehicle = JSON.parse(vehicleJson);
      for (let driver of drivers) {
        const person = await db.query("SELECT id FROM person WHERE identity_no = $1 AND deleted = false", [driver["identity_no"]]);
        if (person["rowCount"] !== 0) {
          let pid = person.rows[0]["id"];
          log.info("old person" + JSON.stringify(person));
          pids.push(pid);
          log.info(pids);
          const driverId = await db.query("SELECT id FROM drivers WHERE pid = $1 AND deleted = false", [pid]);
          if (driverId["rowCount"] === 0) {
            let did = uuid.v4();
            log.info("new driver" + did);
            await db.query("INSERT INTO drivers (id, vid, pid, is_primary) VALUES ($1, $2, $3, $4)", [did, vid, pid, driver["is_primary"]]);
            vehicle["drivers"].push({
              id: pid,
              name: driver["name"],
              identity_no: driver["identity_no"],
              phone: driver["phone"],
              is_primary: driver["is_primary"]
            });
          }
          log.info(JSON.stringify(vehicle));
        } else {
          let pid = uuid.v4();
          let did = uuid.v4();
          log.info("new person and new dirver " + pid + " and " + did);
          pids.push(pid);
          log.info("pids: " + pids);
          await db.query("INSERT INTO person (id, name, identity_no,phone) VALUES ($1, $2, $3, $4)", [pid, driver["name"], driver["identity_no"], driver["phone"]]);
          await db.query("INSERT INTO drivers (id, vid, pid, is_primary) VALUES ($1, $2, $3, $4)", [did, vid, pid, driver["is_primary"]]);
          vehicle["drivers"].push({
            id: pid,
            name: driver["name"],
            identity_no: driver["identity_no"],
            phone: driver["phone"],
            is_primary: driver["is_primary"]
          });
          log.info(JSON.stringify(vehicle));
        }
      }
      await db.query("COMMIT");
      await cache.hsetAsync("vehicle-entities", vid, JSON.stringify(vehicle));
      await cache.setexAsync(callback, 30, JSON.stringify({ code: 200, data: pids }));
      done();
    } catch (e) {
      log.error(e);
      await db.query("ROLLBACK");
      await cache.setexAsync(callback, 30, JSON.stringify({ code: 500, msg: e.message }));
      done();
    }
  })();
});

processor.call("uploadDriverImages", (db: PGClient, cache: RedisClient, done: DoneFunction, vid: string, driving_frontal_view: string, driving_rear_view: string, identity_frontal_view: string, identity_rear_view: string, license_frontal_views: Object, callback: string) => {
  log.info("uploadDriverImages");
  let pbegin = new Promise<void>((resolve, reject) => {
    db.query("BEGIN", [], (err: Error) => {
      if (err) {
        log.error(err);
        reject(err);
      } else {
        resolve();
      }
    });
  });
  let pvehicles = new Promise<void>((resolve, reject) => {
    db.query("UPDATE vehicles SET driving_frontal_view = $1, driving_rear_view = $2 WHERE id = $3", [driving_frontal_view, driving_rear_view, vid], (err: Error) => {
      if (err) {
        log.error(err);
        reject(err);
      } else {
        resolve();
      }
    });
  });
  let pperson = new Promise<void>((resolve, reject) => {
    db.query("UPDATE person SET identity_frontal_view = $1, identity_rear_view = $2 WHERE id in (SELECT owner FROM vehicles WHERE id = $3)", [identity_frontal_view, identity_rear_view, vid], (err: Error) => {
      if (err) {
        log.error(err);
        reject(err);
      } else {
        resolve();
      }
    });
  });
  let pcommit = new Promise<void>((resolve, reject) => {
    db.query("COMMIT", [], (err: Error) => {
      if (err) {
        log.info(err);
        reject(err);
      } else {
        resolve();
      }
    });
  });
  let ps = [pbegin, pvehicles, pperson];
  for (let key in license_frontal_views) {
    if (license_frontal_views.hasOwnProperty(key)) {
      let p = new Promise<void>((resolve, reject) => {
        db.query("UPDATE person SET license_frontal_view=$1 WHERE id = $2", [license_frontal_views[key], key], (err: Error) => {
          if (err) {
            log.error(err);
            reject(err);
          } else {
            resolve();
          }
        });
      });
      ps.push(p);
    }
  }
  ps.push(pcommit);

  async_serial<void>(ps, [], () => {
    cache.hget("vehicle-entities", vid, (err, vehiclejson) => {
      if (err) {
        log.error(err);
        cache.setex(callback, 30, JSON.stringify({
          code: 500,
          msg: err.message
        }), (err, result) => {
          done();
        });
      } else if (vehiclejson) {
        const vehicle = JSON.parse(vehiclejson);
        vehicle["driving_frontal_view"] = driving_frontal_view;
        vehicle["driving_rear_view"] = driving_rear_view;
        vehicle["owner"]["identity_frontal_view"] = identity_frontal_view;
        vehicle["owner"]["identity_rear_view"] = identity_rear_view;
        vehicle["owner"]["license_view"] = license_frontal_views[vehicle["owner"]["id"]];
        for (let key in license_frontal_views) {
          for (let driver of vehicle["drivers"]) {
            if (driver["id"] === key) {
              driver["license_view"] = license_frontal_views[key];
            }
          }
        }
        vehicle_trigger.send(msgpack.encode({ vid, vehicle }));
        cache.hset("vehicle-entities", vid, JSON.stringify(vehicle), (err1, reply) => {
          if (err1) {
            cache.setex(callback, 30, JSON.stringify({
              code: 500,
              msg: err1.message
            }), (err, result) => {
              done();
            });
          } else {
            cache.setex(callback, 30, JSON.stringify({
              code: 200,
              msg: "Success"
            }), (err, result) => {
              done();
            });
          }
        });
      } else {
        cache.setex(callback, 30, JSON.stringify({
          code: 404,
          msg: "Vehicle not found"
        }), (err, result) => {
          done();
        });
      }
    });
  }, (e: Error) => {
    db.query("ROLLBACK", [], (err: Error) => {
      if (err) {
        log.error(err);
      }
      log.error(e, e.message);
      cache.setex(callback, 30, JSON.stringify({
        code: 500,
        msg: e.message
      }), (err, result) => {
        done();
      });
    });
  });
});

interface InsertModelCtx {
  cache: RedisClient;
  db: PGClient;
  done: DoneFunction;
  vin: string;
  models: Object[];
};

processor.call("getVehicleModelsByMake", (db: PGClient, cache: RedisClient, done: DoneFunction, args2: any, vin: string, callback: string) => {
  log.info("getVehicleModelsByMake " + vin);
  (async () => {
    try {
      await db.query("BEGIN");
      let models = args2.vehicleList.map(e => e);
      for (let model of models) {
        await db.query("INSERT INTO vehicle_model(vehicle_code, vehicle_name, brand_name, family_name, body_type, engine_desc, gearbox_name, year_pattern, group_name, cfg_level, purchase_price, purchase_price_tax, seat, effluent_standard, pl, fuel_jet_type, driven_type) VALUES($1, $2, $3, $4, $5, $6, $7, $8 ,$9, $10, $11, $12, $13, $14, $15, $16, $17)", [model["vehicleCode"], model["vehicleName"], model["brandName"], model["familyName"], model["bodyType"], model["engineDesc"], model["gearboxName"], model["yearPattern"], model["groupName"], model["cfgLevel"], model["purchasePrice"], model["purchasePriceTax"], model["seat"], model["effluentStandard"], model["pl"], model["fuelJetType"], model["drivenType"]]);
      }
      await db.query("COMMIT");
      let multi = bluebird.promisifyAll(cache.multi()) as Multi;
      let codes = [];
      for (let model of models) {
        model["vin_code"] = vin;
        multi.hset("vehicle-model-entities", model["vehicleCode"], JSON.stringify(model));
        codes.push(model["vehicleCode"]);
      }
      multi.hset("vehicle-vin-codes", vin, JSON.stringify(codes));
      multi.sadd("vehicle-model", vin);
      await multi.execAsync();
      await cache.setex(callback, 30, JSON.stringify({ code: 200, data: vin }));
      done();
      log.info("getVehicleModelsByMake success");
    } catch (e) {
      log.error(e);
      await db.query("ROLLBACK");
      await cache.setexAsync(callback, 30, JSON.stringify({ code: 500, msg: e.message }));
      done();
    }
  })();
});

function row2model(row: Object) {
  return {
    vehicleCode: row["vehicle_code"] ? row["vehicle_code"].trim() : "",
    vin_code: row["vin_code"] ? row["vin_code"].trim() : "",
    vehicleName: row["vehicle_name"] ? row["vehicle_name"].trim() : "",
    brandName: row["brand_name"] ? row["brand_name"].trim() : "",
    familyName: row["family_name"] ? row["family_name"].trim() : "",
    bodyType: row["body_type"] ? row["body_type"].trim() : "",
    engineDesc: row["engine_desc"] ? row["engine_desc"].trim() : "",
    gearboxName: row["gearbox_name"] ? row["gearbox_name"].trim() : "",
    yearPattern: row["yearPattern"] ? row["yearPattern"].trim() : "",
    groupName: row["group_name"] ? row["group_name"].trim() : "",
    cfgLevel: row["cfg_level"] ? row["cfg_level"].trim() : "",
    purchasePrice: row["purchase_price"],
    purchasePriceTax: row["purchase_price_tax"],
    seat: row["seat"],
    effluentStandard: row["effluent_standard"] ? row["effluent_standard"].trim() : "",
    pl: row["pl"] ? row["pl"].trim() : "",
    fuelJetType: row["fuel_jet_type"] ? row["fuel_jet_type"].trim() : "",
    drivenType: row["driven_type"] ? row["driven_type"].trim() : ""
  };
}

function row2vehicle(row: Object) {
  return {
    id: row["id"] ? row["id"].trim() : "",
    user_id: row["user_id"] ? row["user_id"].trim : "",
    owner: row["owner"] ? row["owner"].trim : "",
    owner_type: row["owner_type"] ? row["owner_type"].trim : "",
    vehicle_code: row["vehicle_code"] ? row["vehicle_code"].trim : "",
    license_no: row["license_no"] ? row["license_no"].trim : "",
    engine_no: row["engine_no"] ? row["engine_no"].trim : "",
    register_date: row["register_date"] ? row["register_date"].trim : "",
    average_mileage: row["average_mileage"] ? row["average_mileage"].trim : "",
    is_transfer: row["is_transfer"] ? row["is_transfer"].trim : "",
    receipt_no: row["receipt_no"] ? row["receipt_no"].trim : "",
    receipt_date: row["receipt_date"] ? row["receipt_date"].trim : "",
    last_insurance_company: row["last_insurance_company"] ? row["last_insurance_company"].trim : "",
    insurance_due_date: row["insurance_due_date"] ? row["insurance_due_date"].trim : "",
    driving_frontal_view: row["driving_frontal_view"] ? row["driving_frontal_view"].trim : "",
    driving_real_view: row["driving_real_view"] ? row["driving_real_view"].trim : "",
    created_at: row["created_at"] ? row["created_at"].trim : "",
    updated_at: row["updated_at"] ? row["updated_at"].trim : "",
    recommend: row["recommend"] ? row["recommend"].trim : "",
    fuel_type: row["fuel_type"] ? row["fuel_type"].trim : "",
  };
}




function refresh_vehicle(db: PGClient, cache: RedisClient, domain: string) {
  log.info("refresh_vehicle");
  return new Promise<void>((resolve, reject) => {
    db.query("SELECT distinct on (d.vid, d.pid) d.id, v.id AS v_id, v.user_id AS v_user_id, v.owner AS v_owner, v.owner_type AS v_owner_type , v.vehicle_code AS v_vehicle_code, v.license_no AS v_license_no,  v.engine_no AS v_engine_no, v.register_date AS v_register_date, v.average_mileage AS v_average_mileage, v.is_transfer AS v_is_transfer, v.receipt_no AS v_receipt_no, v.receipt_date AS v_receipt_data, v.last_insurance_company AS v_last_insurance_company, v.insurance_due_date AS v_insurance_due_date, v.driving_frontal_view AS v_driving_frontal_view, v.driving_rear_view AS v_driving_rear_view, v.created_at AS v_created_at, v.updated_at AS v_updated_at, v.recommend AS v_recommend, v.fuel_type AS v_fuel_type, m.vehicle_code AS m_vehicle_code, v.vin AS m_vin_code, m.vehicle_name AS m_vehicle_name, m.brand_name AS m_brand_name, m.family_name AS m_family_name, m.body_type AS m_body_type,  m.engine_desc AS m_engine_desc, m.gearbox_name AS m_gearbox_name, m.year_pattern AS m_year_pattern, m.group_name AS m_group_name, m.cfg_level AS m_cfg_level, m.purchase_price AS m_purchase_price, m.purchase_price_tax AS m_purchase_price_tax, m.seat AS m_seat, m.effluent_standard AS m_effluent_standard, m.pl AS m_pl, m.fuel_jet_type AS m_fuel_jet_type, m.driven_type AS m_driven_type,p.id AS p_id, p.name AS p_name, p.identity_no AS p_identity, p.phone AS p_phone, p.identity_frontal_view AS p_identity_frontal_view, p.identity_rear_view AS p_identity_rear_view, p.license_frontal_view AS p_license_frontal_view, p.license_rear_view AS p_license_rear_view FROM vehicles AS v INNER JOIN drivers AS d ON v.id = d.vid INNER JOIN vehicle_model AS m ON v.vehicle_code = m.vehicle_code INNER JOIN person AS p ON d.pid = p.id or v.owner = p.id ORDER BY d.vid, d.pid", [], (e: Error, result: QueryResult) => {
      if (e) {
        reject(e);
        log.info("err : SELECT query error" + e);
      } else {
        const vehicles: Object = {};
        for (const row of result.rows) {
          if (vehicles.hasOwnProperty(row.v_id)) {
            vehicles[row.v_id]["pids"].push(row.d_pid);
          } else {
            let r_date: string;
            if (row.v_register_date) {
              let register = new Date(row.v_register_date);
              r_date = register.getFullYear() + "-" + (register.getMonth() + 1) + "-" + register.getDate();
            }
            const vehicle = {
              id: row.v_id,
              user_id: row.v_user_id,
              owid: row.v_owner,
              owner: {},
              owner_type: row.v_owner_type,
              vehicle_code: trim(row.v_vehicle_code),
              license_no: trim(row.v_license_no),
              engine_no: trim(row.v_engine_no),
              register_date: r_date,
              average_mileage: trim(row.v_average_mileage),
              drivers: [],
              pids: [row.d_pid],
              vin_code: trim(row.m_vin_code),
              vehicle_model: {
                vehicleCode: trim(row.m_vehicle_code),
                vin_code: trim(row.m_vin_code),
                vehicleName: trim(row.m_vehicle_name),
                brandName: trim(row.m_brand_name),
                familyName: trim(row.m_family_name),
                bodyType: trim(row.m_body_type),
                engineDesc: trim(row.m_engine_desc),
                gearboxName: trim(row.m_gearbox_name),
                yearPattern: trim(row.m_year_pattern),
                groupName: trim(row.m_group_name),
                cfgLevel: trim(row.m_cfg_level),
                purchasePrice: row.m_purchase_price,
                purchasePriceTax: row.m_purchase_price_tax,
                seat: row.m_seat,
                effluentStandard: trim(row.m_effluent_standard),
                pl: trim(row.m_pl),
                fuelJetType: trim(row.m_fuel_jet_type),
                drivenType: trim(row.m_driven_type)
              },
              is_transfer: row.v_is_transfer,
              receipt_no: trim(row.v_receipt_no),
              receipt_date: row.v_receipt_data,
              last_insurance_company: trim(row.v_last_insurance_company),
              insurance_due_date: row.v_insurance_due_date,
              driving_frontal_view: trim(row.v_driving_frontal_view),
              driving_real_view: trim(row.v_driving_real_view),
              created_at: row.v_created_at,
              updated_at: row.v_updated_at,
              recommend: trim(row.v_recommend),
              fuel_type: trim(row.v_fuel_type)
            };
            vehicles[row.v_id] = vehicle;
          }
        }
        const vids = Object.keys(vehicles);
        let vehicle_users: Object = {};
        for (let vid of vids) {
          if (vehicle_users.hasOwnProperty(vehicles[vid]["user_id"])) {
            if (!vehicle_users[vehicles[vid]["user_id"]].some(v => v === vid)) {
              vehicle_users[vehicles[vid]["user_id"]].push(vid);
            }
          } else {
            vehicle_users[vehicles[vid]["user_id"]] = [vid];
          }
          for (let pid of vehicles[vid]["pids"]) {
            if (pid !== null) {
              for (let row of result.rows) {
                if (pid === row.p_id) {
                  vehicles[vid]["drivers"].push({
                    id: row.p_id,
                    name: trim(row.p_name),
                    identity_no: trim(row.p_identity),
                    phone: trim(row.phone),
                    identity_front_view: trim(row.p_identity_front_view),
                    identity_rear_view: trim(row.p_identity_rear_view),
                    license_frontal_view: trim(row.p_license_frontal_view),
                    license_rear_view: trim(row.p_license_rear_view)
                  });
                }
              }
            }
          }
        }
        for (let vid of vids) {
          if (vehicles[vid]["owid"] !== null) {
            for (let row of result.rows) {
              if (vehicles[vid]["owid"] === row.p_id) {
                vehicles[vid]["owner"].id = row.p_id;
                vehicles[vid]["owner"].name = trim(row.p_name);
                vehicles[vid]["owner"].identity_no = trim(row.p_identity);
                vehicles[vid]["owner"].phone = trim(row.p_phone);
                vehicles[vid]["owner"].identity_front_view = trim(row.p_identity_front_view);
                vehicles[vid]["owner"].identity_rear_view = trim(row.p_identity_rear_view);
                vehicles[vid]["owner"].license_frontal_view = trim(row.p_license_frontal_view);
                vehicles[vid]["owner"].license_rear_view = trim(row.p_license_rear_view);
                break;
              }
            }
          }
        }
        const multi = cache.multi();
        for (const vid of Object.keys(vehicles)) {
          const vehicle = vehicles[vid];
          delete vehicle["pids"];
          delete vehicle["owid"];
          multi.hset("vehicle-entities", vid, JSON.stringify(vehicle));
          multi.lpush("vehicle", vid);
          // multi.hset("vehicle-vin-codes", vehicle["vehicle_model"]["vin_code"], vehicle["vehicle_model"]["vin_code"]);
          multi.hset("vehicle-model-entities", vehicle["vehicle_model"]["vin_code"], JSON.stringify(vehicle["vehicle_model"]));
          multi.sadd("vehicle-model", vehicle["vehicle_model"]["vin_code"]);
        }
        for (const key of Object.keys(vehicle_users)) {
          multi.lpush("vehicle-" + key, vehicle_users[key]);
        }
        multi.exec((err: Error, _: any[]) => {
          if (err) {
            reject(err);
          } else {
            resolve();
          }
        });
      }
    });
  });
}


processor.call("refresh", (db: PGClient, cache: RedisClient, done: DoneFunction, domain: string) => {
  log.info("refresh");
  const pdo = refresh_vehicle(db, cache, domain);
  let ps = [pdo];
  async_serial_ignore<void>(ps, [], () => {
    log.info("refresh done!");
    done();
  });
});

function trim(str: string) {
  if (str) {
    return str.trim();
  } else {
    return null;
  }
}

// 出险次数
processor.call("damageCount", (db: PGClient, cache: RedisClient, done: DoneFunction, vid: string, count: number, callback: string) => {
  log.info("damageCount ");
  modifyVehicle(db, cache, done, vid, callback, "UPDATE vehicles SET accident_times = $1 WHERE id = $2 and deleted = false", [count, vid], (vehicle) => {
    vehicle["accident_times"] = count;
    return vehicle;
  });
});
function modifyVehicle(db: PGClient, cache: RedisClient, done: DoneFunction, vid: string, cbflag: string, sql: string, args: any[], cb: ((vehicle: Object) => Object)): void {
  new Promise<void>((resolve, reject) => {
    db.query(sql, args, (err: Error) => {
      if (err) {
        log.info(err);
        reject(err);
      } else {
        resolve(null);
      }
    });
  })
    .then(() => {
      return new Promise<Object>((resolve, reject) => {
        log.info("redis " + vid);
        cache.hget("vehicle-entities", vid, function (err, result) {
          if (result) {
            resolve(JSON.parse(result));
          } else if (err) {
            log.info(err);
            reject(err);
          } else {
            resolve(null);
          }
        });
      });
    })
    .then((vehicle: Object) => {
      if (vehicle != null) {
        let uw = cb(vehicle);
        let multi = cache.multi();
        multi.hset("vehicle-entities", vid, JSON.stringify(uw));
        multi.setex(cbflag, 30, JSON.stringify({
          code: 200,
          data: { vid: vid, accident_times: vehicle["accident_times"] }
        }));
        multi.exec((err: Error, _) => {
          if (err) {
            log.error(err, "update vehicle cache error");
            cache.setex(cbflag, 30, JSON.stringify({
              code: 500,
              msg: "update vehicle cache error"
            }));
          } else {
            log.info("success");
          }
          vehicle_trigger.send(msgpack.encode({ vid, vehicle }));
          done();
        });
      } else {
        cache.setex(cbflag, 30, JSON.stringify({
          code: 404,
          msg: "Not found vehicle"
        }));
        log.info("Not found vehicle");
        done();
      }
    })
    .catch(error => {
      cache.setex(cbflag, 30, JSON.stringify({
        code: 500,
        msg: error.message
      }), (err, result) => {
        done();
      });
      log.info("err" + error);
    });
}
log.info("Start processor at %s", config.addr);

processor.run();

