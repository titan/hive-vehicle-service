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
    lpushAsync(key: string, field: string): Promise<any>;
    lrangeAsync(key: string, field: number, field2: number): Promise<any>;
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
      if (person["rowCount"] !== 0) {
        pid = person.rows[0]["id"];
        log.info("old perosn: " + pid);
        owner = {
          id: pid,
          name: trim(person.rows[0]["name"]),
          identity_no: trim(person.rows[0]["identity_no"]),
          phone: trim(person.rows[0]["phone"])
        };
      } else {
        pid = uuid.v1();
        log.info("new perosn: " + pid);
        await db.query("INSERT INTO person (id, name, identity_no, phone) VALUES ($1, $2, $3, $4)", [pid, name, identity_no, phone]);
        owner = {
          id: pid,
          name: name,
          identity_no: identity_no,
          phone: phone
        };
      }
      const vids = await db.query("SELECT id FROM vehicles WHERE vin = $1 AND deleted = false", [vin]);
      let vid = "";
      if (vids["rowCount"] !== 0) {
        vid = vids.rows[0]["id"];
        log.info("old vehicle id: " + vid);
        await db.query("UPDATE vehicles SET owner = $1, uid = $2 WHERE id = $3 AND deleted = false", [pid, uid, vid]);
        const vehicleJson = await cache.hgetAsync("vehicle-entities", vid);
        let vehicle = JSON.parse(vehicleJson);
        vehicle["owner"] = owner;
        vehicle["uid"] = uid;
        const userVids = await cache.lrangeAsync("vehicle-" + uid, 0, -1);
        if (!userVids.some(id => id === vid)) {
          await cache.lpushAsync("vehicle-" + uid, vid);
        }
        await cache.hsetAsync("vehicle-entities", vid, JSON.stringify(vehicle));

      } else {
        vid = uuid.v1();
        log.info("new vehicle id: " + vid);
        await db.query("INSERT INTO vehicles (id, uid, owner, owner_type, recommend, vehicle_code,license_no,engine_no,register_date,average_mileage,is_transfer, last_insurance_company,insurance_due_date, fuel_type, vin) VALUES ($1, $2, $3, $4, $5, $6, $7, $8 ,$9, $10 ,$11, $12, $13, $14, $15)", [vid, uid, pid, 0, recommend, vehicle_code, license_no, engine_no, register_date, average_mileage, is_transfer, last_insurance_company, insurance_due_date, fuel_type, vin]);
        let vehicle = {
          id: vid,
          uid: uid,
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
      if (person["rowCount"] !== 0) {
        pid = person.rows[0]["id"];
        log.info("old person: " + pid);
        owner = {
          id: pid,
          name: trim(person.rows[0]["name"]),
          identity_no: trim(person.rows[0]["identity_no"]),
          phone: trim(person.rows[0]["phone"])
        };
      } else {
        pid = uuid.v1();
        log.info("new perosn: " + pid);
        owner = {
          id: pid,
          name: name,
          identity_no: identity_no,
          phone: phone
        };
        await db.query("INSERT INTO person (id, name, identity_no, phone) VALUES ($1, $2, $3, $4)", [pid, name, identity_no, phone]);
      }
      const vids = await db.query("SELECT id FROM vehicles WHERE vin = $1 AND deleted = false", [vin]);
      let vid = "";
      if (vids["rowCount"] !== 0) {
        vid = vids.rows[0]["id"];
        log.info("old vehicle id: " + vid);
        await db.query("UPDATE vehicles SET owner = $1, uid = $2 WHERE id = $3 AND deleted = false", [pid, uid, vid]);
        const vehicleJson = await cache.hgetAsync("vehicle-entities", vid);
        let vehicle = JSON.parse(vehicleJson);
        vehicle["owner"] = owner;
        vehicle["uid"] = uid;
        const userVids = await cache.lrangeAsync("vehicle-" + uid, 0, -1);
        if (!userVids.some(id => id === vid)) {
          await cache.lpushAsync("vehicle-" + uid, vid);
        }
        await cache.hsetAsync("vehicle-entities", vid, JSON.stringify(vehicle));
      } else {
        vid = uuid.v1();
        log.info("new vehicle id: " + vid);
        await db.query("INSERT INTO vehicles (id, uid, owner, owner_type, recommend, vehicle_code, engine_no,average_mileage,is_transfer,receipt_no, receipt_date,last_insurance_company, fuel_type, vin) VALUES ($1, $2, $3, $4, $5, $6, $7, $8 ,$9, $10, $11, $12, $13, $14)", [vid, uid, pid, 0, recommend, vehicle_code, engine_no, average_mileage, is_transfer, receipt_no, receipt_date, last_insurance_company, fuel_type, vin]);
        let vehicle = {
          id: vid,
          uid: uid,
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

processor.call("addDrivers", (db: PGClient, cache: RedisClient, done: DoneFunction, vid: string, drivers: any, callback: string) => {
  log.info("addDrivers " + vid);
  (async () => {
    try {
      let pids = [];
      await db.query("BEGIN");
      let vehicleJson = await cache.hgetAsync("vehicle-entities", vid);
      let vehicle = JSON.parse(vehicleJson);
      for (let driver of drivers) {
        const person = await db.query("SELECT id, name, identity_no, phone FROM person WHERE identity_no = $1 AND deleted = false", [driver["identity_no"]]);
        if (person["rowCount"] !== 0) {
          let pid = person.rows[0]["id"];
          log.info("old person" + JSON.stringify(person));
          pids.push(pid);
          const driverId = await db.query("SELECT id FROM drivers WHERE pid = $1 AND vid = $2 AND deleted = false", [pid, vid]);
          if (driverId["rowCount"] === 0) {
            let did = uuid.v4();
            log.info("new driver" + did);
            await db.query("INSERT INTO drivers (id, vid, pid, is_primary) VALUES ($1, $2, $3, $4)", [did, vid, pid, driver["is_primary"]]);
            vehicle["drivers"].push({
              id: pid,
              name: trim(person.rows[0]["name"]),
              identity_no: trim(person.rows[0]["identity_no"]),
              phone: trim(person.rows[0]["phone"]),
              is_primary: driver["is_primary"]
            });
          }
          log.info(JSON.stringify(vehicle));
        } else {
          let pid = uuid.v4();
          let did = uuid.v4();
          log.info("new person and new dirver " + pid + " and " + did);
          pids.push(pid);
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
      log.info("pids ==> " + pids);
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
              data: vid
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
        let dbmodel = await db.query("SELECT * FROM vehicle_models WHERE vehicle_code = $1", [model["vehicleCode"]]);
        if (dbmodel["rowCount"] === 0) {
          await db.query("INSERT INTO vehicle_models(vehicle_code, vehicle_name, brand_name, family_name, body_type, engine_desc, gearbox_name, year_pattern, group_name, cfg_level, purchase_price, purchase_price_tax, seat, effluent_standard, pl, fuel_jet_type, driven_type) VALUES($1, $2, $3, $4, $5, $6, $7, $8 ,$9, $10, $11, $12, $13, $14, $15, $16, $17)", [model["vehicleCode"], model["vehicleName"], model["brandName"], model["familyName"], model["bodyType"], model["engineDesc"], model["gearboxName"], model["yearPattern"], model["groupName"], model["cfgLevel"], model["purchasePrice"], model["purchasePriceTax"], model["seat"], model["effluentStandard"], model["pl"], model["fuelJetType"], model["drivenType"]]);
        }
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
      await cache.setexAsync(callback, 30, JSON.stringify({ code: 200, data: args2.vehicleList }));
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
    vin_code: row["vin"] ? row["vin"].trim() : "",
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
    uid: row["uid"] ? row["uid"].trim() : "",
    owner_type: row["owner_type"] ? row["owner_type"].trim() : "",
    vehicle_code: row["vehicle_code"] ? row["vehicle_code"].trim() : "",
    license_no: row["license_no"] ? row["license_no"].trim() : "",
    engine_no: row["engine_no"] ? row["engine_no"].trim() : "",
    register_date: row["register_date"],
    average_mileage: row["average_mileage"] ? row["average_mileage"].trim() : "",
    is_transfer: row["is_transfer"] ? row["is_transfer"].trim() : "",
    receipt_no: row["receipt_no"] ? row["receipt_no"].trim() : "",
    receipt_date: row["receipt_date"],
    last_insurance_company: row["last_insurance_company"] ? row["last_insurance_company"].trim() : "",
    insurance_due_date: row["insurance_due_date"],
    driving_frontal_view: row["driving_frontal_view"] ? row["driving_frontal_view"].trim() : "",
    driving_real_view: row["driving_real_view"] ? row["driving_real_view"].trim() : "",
    recommend: row["recommend"] ? row["recommend"].trim() : "",
    fuel_type: row["fuel_type"] ? row["fuel_type"].trim() : "",
    accident_times: row["accident_times"],
    vin_code: row["vin"] ? row["vin"].trim() : "",
    created_at: row["created_at"],
    updated_at: row["updated_at"],
  };
}

function row2person(row: Object) {
  return {
    id: row["pid"] ? row["pid"].trim() : "",
    name: row["name"] ? row["name"].trim() : "",
    identity_no: row["identity_no"] ? row["identity_no"].trim() : "",
    phone: row["phone"] ? row["phone"].trim() : "",
    identity_front_view: row["identity_front_view"] ? row["identity_front_view"].trim() : "",
    identity_rear_view: row["identity_rear_view"] ? row["identity_rear_view"].trim() : "",
    license_frontal_view: row["license_frontal_view"] ? row["license_frontal_view"].trim() : "",
    license_rear_view: row["license_rear_view"] ? row["license_rear_view"].trim() : "",
  }
}

function row2driver(row: Object) {
  return {
    id: row["pid"] ? row["pid"].trim() : "",
    name: row["name"] ? row["name"].trim() : "",
    identity_no: row["identity_no"] ? row["identity_no"].trim() : "",
    phone: row["phone"] ? row["phone"].trim() : "",
    identity_front_view: row["identity_front_view"] ? row["identity_front_view"].trim() : "",
    identity_rear_view: row["identity_rear_view"] ? row["identity_rear_view"].trim() : "",
    license_frontal_view: row["license_frontal_view"] ? row["license_frontal_view"].trim() : "",
    license_rear_view: row["license_rear_view"] ? row["license_rear_view"].trim() : "",
    is_primary: row["is_primary"],
    created_at: row["created_at"],
    updated_at: row["updated_at"],
  }
}

function trim(str: string) {
  if (str) {
    return str.trim();
  } else {
    return null;
  }
}

processor.call("refresh", (db: PGClient, cache: RedisClient, done: DoneFunction, callback: string) => {
  log.info("refresh " + callback);
  (async () => {
    try {
      const dbVehicleModel = await db.query("SELECT vin, vehicles.vehicle_code AS vehicle_code, vehicle_name, brand_name, family_name, body_type, engine_desc, gearbox_name, year_pattern, group_name, cfg_level, purchase_price, purchase_price_tax, seat, effluent_standard, pl, fuel_jet_type, driven_type FROM vehicle_models, vehicles WHERE vehicles.vehicle_code = vehicle_models.vehicle_code", []);
      const dbVehicle = await db.query("SELECT v.id AS id, uid, owner, owner_type, vehicle_code, license_no, engine_no, register_date, average_mileage, is_transfer, receipt_no, receipt_date, last_insurance_company, insurance_due_date, driving_frontal_view, driving_rear_view, recommend, fuel_type, accident_times, vin, v.created_at AS created_at, v.updated_at AS updated_at, p.id AS pid, name, identity_no, phone, identity_frontal_view, identity_rear_view, license_frontal_view, license_rear_view FROM vehicles AS v, person AS p WHERE p.id = v.owner")
      const dbDriver = await db.query("SELECT p.id AS pid, v.id AS vid, name, identity_no, phone, identity_frontal_view, identity_rear_view, license_frontal_view, license_rear_view, is_primary, d.created_at AS created_at, d.updated_at AS updated_at FROM drivers AS d, person AS p, vehicles AS v WHERE p.id = d.pid AND d.vid = v.id ORDER BY v.id");
      let vehicle_models = dbVehicleModel.rows;
      let vehicles = dbVehicle.rows;
      let drivers = dbDriver.rows;
      let multi = bluebird.promisifyAll(cache.multi()) as Multi;
      let vehicleJsons = [];
      for (let vehicle of vehicles) {
        for (let vehicleModel of vehicle_models) {
          let vin = trim(vehicleModel["vin"]);
          if (trim(vehicle["vin"]) === vin) {
            let vehicleCode = trim(vehicleModel["vehicle_code"]);
            let vehicleModelJson = row2model(vehicleModel);
            let vehicleJson = row2vehicle(vehicle);
            let vid = trim(vehicle["id"]);
            let vehicleCodeJson = [];
            vehicleJson["vehicle_model"] = vehicleModelJson;
            vehicleJson["owner"] = row2person(vehicle);
            vehicleJson["drivers"] = [];
            vehicleJsons.push(vehicleJson);
            vehicleCodeJson.push(vehicleCode)
            multi.lpush("vehicle", vid);
            multi.sadd("vehicle-model", vin);
            multi.hset("vehicle-vin-codes", vin, JSON.stringify(vehicleCodeJson));
            multi.hset("vehicle-model-entities", vehicleCode, JSON.stringify(vehicleModelJson));
          }
        }
      }
      let vehicleUsers: Object = {};
      for (let vehicle of vehicleJsons) {
        if (vehicleUsers.hasOwnProperty(vehicle["uid"])) {
          if (!vehicleUsers[vehicle["uid"]].some(v => v === vehicle["id"])) {
            vehicleUsers[vehicle["uid"]].push(vehicle["id"]);
          }
        } else {
          vehicleUsers[vehicle["uid"]] = [vehicle["id"]];
        }
        for (let driver of drivers) {
          let vid = trim(vehicle["id"]);
          let dvid = trim(driver["vid"]);
          if (vid === dvid) {
            vehicle["drivers"].push(row2driver(driver));
            multi.hset("vehicle-entities", vid, JSON.stringify(vehicle));
          }
        }
      }
      for (const key of Object.keys(vehicleUsers)) {
        multi.lpush("vehicle-" + key, vehicleUsers[key]);
      }
      await multi.execAsync();
      await cache.setexAsync(callback, 30, JSON.stringify({ code: 200, data: "refresh success" }));
      done();
      log.info("refresh success");
    } catch (e) {
      log.error(e);
      await cache.setexAsync(callback, 30, JSON.stringify({ code: 500, msg: e.message }));
      done();
    }
  })();
});

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

processor.call("addVehicleModels", (db: PGClient, cache: RedisClient, done: DoneFunction, vin: string, vehicle_models: Object[], callback: string) => {
  log.info("addVehicleModels");
  (async () => {
    try {
      await db.query("BEGIN");
      for (let model of vehicle_models) {
        let dbmodel = await db.query("SELECT * FROM vehicle_models WHERE vehicle_code = $1", [model["vehicleCode"]]);
        if (dbmodel["rowCount"] === 0) {
          await db.query("INSERT INTO vehicle_models(vehicle_code, vehicle_name, brand_name, family_name, body_type, engine_desc, gearbox_name, year_pattern, group_name, cfg_level, purchase_price, purchase_price_tax, seat, effluent_standard, pl, fuel_jet_type, driven_type) VALUES($1, $2, $3, $4, $5, $6, $7, $8 ,$9, $10, $11, $12, $13, $14, $15, $16, $17)", [model["vehicleCode"], model["vehicleName"], model["brandName"], model["familyName"], model["bodyType"], model["engineDesc"], model["gearboxName"], model["yearPattern"], model["groupName"], model["cfgLevel"], model["purchasePrice"], model["purchasePriceTax"], model["seat"], model["effluentStandard"], model["pl"], model["fuelJetType"], model["drivenType"]]);
        }
      }
      await db.query("COMMIT");
      let multi = bluebird.promisifyAll(cache.multi()) as Multi;
      let codes = [];
      for (let model of vehicle_models) {
        model["vin_code"] = vin;
        multi.hset("vehicle-model-entities", model["vehicleCode"], JSON.stringify(model));
        codes.push(model["vehicleCode"]);
      }
      multi.hset("vehicle-vin-codes", vin, JSON.stringify(codes));
      multi.sadd("vehicle-model", vin);
      await multi.execAsync();
      await cache.setexAsync(callback, 30, JSON.stringify({ code: 200, data: codes }));
      done();
      log.info("addVehicleModels success");
    } catch (e) {
      log.error(e);
      await db.query("ROLLBACK");
      await cache.setexAsync(callback, 30, JSON.stringify({ code: 500, msg: e.message }));
      done();
    }
  })();
});
