import { Processor, ProcessorFunction, ProcessorContext, rpc, set_for_response, msgpack_decode, msgpack_encode } from "hive-service";
import { Client as PGClient, QueryResult } from "pg";
import { RedisClient, Multi } from "redis";
import * as uuid from "node-uuid";
import * as http from "http";
import * as bluebird from "bluebird";
import * as bunyan from "bunyan";

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

export const processor = new Processor();

// 新车已上牌个人
processor.call("setVehicleOnCard", (ctx: ProcessorContext, name: string, identity_no: string, phone: string, uid: string, recommend: string, vehicle_code: string, license_no: string, engine_no: string,
  register_date: any, average_mileage: string, is_transfer: boolean, last_insurance_company: string, insurance_due_date: any, fuel_type: string, vin: string, accident_status: number, callback: string) => {
  log.info("setVehicleOnCard");
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const done = ctx.done;
  (async () => {
    try {
      let pid = "";
      let owner = {};
      await db.query("BEGIN");
      const person = await db.query("SELECT id, name, identity_no, phone FROM person WHERE identity_no = $1 AND deleted = false", [identity_no]);
      if (person["rowCount"] !== 0) {
        pid = person.rows[0]["id"];
        log.info("old perosn: " + pid);
        const personValid = await db.query("SELECT 1 FROM person WHERE identity_no = $1 AND deleted = false AND verified = true", [identity_no]);
        if (personValid["rowCount"] !== 0) {
          owner = {
            id: pid,
            name: person.rows[0]["name"].trim(),
            identity_no: person.rows[0]["identity_no"].trim(),
            phone: person.rows[0]["phone"].trim(),
            verified: true
          };
        } else {
          await db.query("UPDATE person SET name = $1, phone = $2 WHERE identity_no = $3 AND deleted = false", [name, phone, identity_no]);
          owner = {
            id: pid,
            name: name,
            identity_no: identity_no,
            phone: phone,
            verified: false
          };
        }
      } else {
        pid = uuid.v1();
        log.info("new perosn: " + pid);
        await db.query("INSERT INTO person (id, name, identity_no, phone) VALUES ($1, $2, $3, $4)", [pid, name, identity_no, phone]);
        owner = {
          id: pid,
          name: name,
          identity_no: identity_no,
          phone: phone,
          verified: false
        };
      }
      let vid = uuid.v1();
      log.info("new vehicle id: " + vid);
      await db.query("INSERT INTO vehicles (id, uid, owner, owner_type, recommend, vehicle_code, license_no,engine_no, register_date, average_mileage, is_transfer, last_insurance_company, insurance_due_date, fuel_type, vin, accident_status) VALUES ($1, $2, $3, $4, $5, $6, $7, $8 ,$9, $10 ,$11, $12, $13, $14, $15, $16)", [vid, uid, pid, 0, recommend, vehicle_code, license_no, engine_no, register_date, average_mileage, is_transfer, last_insurance_company, insurance_due_date, fuel_type, vin, accident_status]);
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
        fuel_type: fuel_type,
        accident_status: accident_status
      };
      const vehicle_model_json = await cache.hgetAsync("vehicle-model-entities", vehicle_code);
      vehicle["vehicle_model"] = await msgpack_decode(vehicle_model_json);
      let multi = bluebird.promisifyAll(cache.multi()) as Multi;
      const pkt = await msgpack_encode(vehicle);
      multi.hset("vehicle-entities", vid, pkt);
      multi.lpush("vehicle-" + uid, vid);
      multi.lpush("vehicle", vid);
      await multi.execAsync();
      await db.query("COMMIT");
      await set_for_response(cache, callback, { code: 200, data: vid });
      done();
    } catch (e) {
      log.error(e);
      await db.query("ROLLBACK");
      set_for_response(cache, callback, { code: 500, msg: e.message }).then(_ => {
        done();
      }).catch(e => {
        log.error(e);
        done();
      });
    }
  })();
});

// 新车未上牌个人
processor.callAsync("setVehicle", async (ctx: ProcessorContext, uid: string, owner_name: string, owner_identity_no: string, owner_phone: string, applicant_name: string, applicant_identity_no: string, applicant_phone: string, recommend: string, vehicle_code: string, engine_no: string, average_mileage: string, is_transfer: boolean, receipt_no: string, receipt_date: any, last_insurance_company: string, fuel_type: string, vin: string) => {
  log.info(`setVehicle, uid: ${uid}, owner_name: ${owner_name}, owner_identity_no: ${owner_identity_no}, owner_phone: ${owner_phone}, applicant_name: ${applicant_name}, applicant_identity_no: ${applicant_identity_no}, applicant_phone: ${applicant_phone}, recommend: ${recommend}, vehicle_code: ${vehicle_code}, engine_no: ${engine_no}, average_mileage: ${average_mileage}, is_transfer: ${is_transfer}, receipt_no: ${receipt_no}, receipt_date: ${receipt_date}, last_insurance_company: ${last_insurance_company}, fuel_type: ${fuel_type}, vin: ${vin}`);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  try {
    const pids = [];
    const params = [[owner_identity_no, owner_name, owner_phone], [applicant_identity_no, applicant_name, applicant_phone]];
    await db.query("BEGIN");
    for (const param of params) {
      const identity_no = param[0];
      const name = param[1];
      const phone = param[2];
      const presult = await db.query("SELECT id, name, identity_no, phone, verified FROM person WHERE identity_no = $1 AND deleted = false", [identity_no]);
      if (presult["rowCount"] !== 0) {
        const pid = presult.rows[0]["id"];
        if (!presult.rows[0].verified) {
          await db.query("UPDATE person SET name = $1, phone = $2 WHERE identity_no = $3 AND deleted = false", [name, phone, identity_no]);
        }
        pids.push(pid);
      } else {
        const pid = uuid.v1();
        await db.query("INSERT INTO person (id, name, identity_no, phone) VALUES ($1, $2, $3, $4)", [pid, name, identity_no, phone]);
        pids.push(pid);
      }
    }
    const vid = uuid.v1();
    await db.query("INSERT INTO vehicles (id, uid, owner, owner_type, recommend, vehicle_code, engine_no,average_mileage,is_transfer,receipt_no, receipt_date,last_insurance_company, fuel_type, vin, applicant) VALUES ($1, $2, $3, $4, $5, $6, $7, $8 ,$9, $10, $11, $12, $13, $14, $15)", [vid, uid, pids[0], 0, recommend, vehicle_code, engine_no, average_mileage, is_transfer, receipt_no, receipt_date, last_insurance_company, fuel_type, vin, pids[1]]);
    sync_vehicle(db, cache, vid);
    await db.query("COMMIT");
    return { code: 200, data: vid };
  } catch (e) {
    log.error(e);
    await db.query("ROLLBACK");
    return { code: 500, msg: e.message };
  }
});

processor.call("addDrivers", (ctx: ProcessorContext, vid: string, drivers: any, callback: string) => {
  log.info("addDrivers " + vid);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const done = ctx.done;
  (async () => {
    try {
      let pids = [];
      await db.query("BEGIN");
      let vehicleJson = await cache.hgetAsync("vehicle-entities", vid);
      let vehicle = await msgpack_decode(vehicleJson);
      for (let driver of drivers) {
        const person = await db.query("SELECT id, name, identity_no, phone FROM person WHERE identity_no = $1 AND deleted = false", [driver["identity_no"]]);
        if (person["rowCount"] !== 0) {
          let pid = person.rows[0]["id"];
          pids.push(pid);
          const personValid = await db.query("SELECT 1 FROM person WHERE identity_no = $1 AND deleted = false AND verified = true", [driver["identity_no"]]);
          let dname = "";
          let didentity_no = "";
          let dphone = "";
          if (personValid["rowCount"] !== 0) {
            dname = person.rows[0]["name"].trim();
            didentity_no = person.rows[0]["identity_no"].trim();
            dphone = person.rows[0]["phone"].trim();
          } else {
            await db.query("UPDATE person SET name = $1 WHERE identity_no = $2 AND deleted = false", [driver["name"], driver["identity_no"]]);
            dname = driver["name"];
            didentity_no = driver["identity_no"];
            dphone = driver["is_primary"];
          }
          const driverId = await db.query("SELECT id FROM drivers WHERE pid = $1 AND vid = $2 AND deleted = false", [pid, vid]);
          if (driverId["rowCount"] === 0) {
            let did = uuid.v4();
            log.info("new driver" + did);
            await db.query("INSERT INTO drivers (id, vid, pid, is_primary) VALUES ($1, $2, $3, $4)", [did, vid, pid, driver["is_primary"]]);
            vehicle["drivers"].push({
              id: pid,
              name: dname,
              identity_no: didentity_no,
              phone: dphone,
              is_primary: driver["is_primary"]
            });
          } else {
            // update driver info
            log.info("driver id is " + pid);
            await db.query("UPDATE drivers SET is_primary = $1", [driver["is_primary"]]);
            for (let d of vehicle["drivers"]) {
              if (pid === d["id"]) {
                d["name"] = dname;
                d["identity_no"] = didentity_no;
                d["phone"] = dphone;
                d["is_primary"] = driver["is_primary"];
                break;
              }
            }
          }
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
        }
      }
      await db.query("COMMIT");
      const pkt = await msgpack_encode(vehicle);
      await cache.hsetAsync("vehicle-entities", vid, pkt);
      log.info("pids ==> " + pids);
      await set_for_response(cache, callback, { code: 200, data: pids });
      done();
    } catch (e) {
      log.error(e);
      await db.query("ROLLBACK");
      set_for_response(cache, callback, { code: 500, msg: e.message }).then(_ => {
        done();
      }).catch(e => {
        log.error(e);
        done();
      });
    }
  })();
});

processor.call("uploadDriverImages", (ctx: ProcessorContext, vid: string, driving_frontal_view: string, driving_rear_view: string, identity_frontal_view: string, identity_rear_view: string, license_frontal_views: Object, callback: string) => {
  log.info("uploadDriverImages");
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const done = ctx.done;
  (async () => {
    try {
      await db.query("BEGIN");
      await db.query("UPDATE vehicles SET driving_frontal_view = $1, driving_rear_view = $2 WHERE id = $3", [driving_frontal_view, driving_rear_view, vid]);
      await db.query("UPDATE person SET identity_frontal_view = $1, identity_rear_view = $2 WHERE id in (SELECT owner FROM vehicles WHERE id = $3)", [identity_frontal_view, identity_rear_view, vid]);
      for (let key in license_frontal_views) {
        if (license_frontal_views.hasOwnProperty(key)) {
          await db.query("UPDATE person SET license_frontal_view=$1 WHERE id = $2", [license_frontal_views[key], key]);
        }
      }
      await db.query("COMMIT");
      const vehicleJson = await cache.hgetAsync("vehicle-entities", vid);
      let vehicle = await msgpack_decode(vehicleJson);
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
      const pkt = await msgpack_encode(vehicle);
      await cache.hsetAsync("vehicle-entities", vid, pkt);
      await set_for_response(cache, callback, { code: 200, data: vid });
      done();
    } catch (e) {
      log.error(e);
      await db.query("ROLLBACK");
      set_for_response(cache, callback, { code: 500, msg: e.message }).then(_ => {
        done();
      }).catch(e => {
        log.error(e);
        done();
      });
    }
  })();
});

processor.call("getVehicleModelsByMake", (ctx: ProcessorContext, args2: any, vin: string, callback: string) => {
  log.info("getVehicleModelsByMake " + vin);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const done = ctx.done;
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
        const pkt = await msgpack_encode(model);
        multi.hset("vehicle-model-entities", model["vehicleCode"], pkt);
        codes.push(model["vehicleCode"]);
      }
      multi.hset("vehicle-vin-codes", vin, JSON.stringify(codes));
      multi.sadd("vehicle-model", vin);
      await multi.execAsync();
      await set_for_response(cache, callback, { code: 200, data: args2.vehicleList });
      done();
      log.info("getVehicleModelsByMake success");
    } catch (e) {
      log.error(e);
      await db.query("ROLLBACK");
      set_for_response(cache, callback, { code: 500, msg: e.message }).then(_ => {
        done();
      }).catch(e => {
        log.error(e);
        done();
      });
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
    owner_type: row["owner_type"],
    vehicle_code: row["vehicle_code"] ? row["vehicle_code"].trim() : "",
    license_no: row["license_no"] ? row["license_no"].trim() : "",
    engine_no: row["engine_no"] ? row["engine_no"].trim() : "",
    register_date: row["register_date"],
    average_mileage: row["average_mileage"] ? row["average_mileage"].trim() : "",
    is_transfer: row["is_transfer"],
    receipt_no: row["receipt_no"] ? row["receipt_no"].trim() : "",
    receipt_date: row["receipt_date"],
    last_insurance_company: row["last_insurance_company"] ? row["last_insurance_company"].trim() : "",
    insurance_due_date: row["insurance_due_date"],
    driving_frontal_view: row["driving_frontal_view"] ? row["driving_frontal_view"].trim() : "",
    driving_real_view: row["driving_real_view"] ? row["driving_real_view"].trim() : "",
    recommend: row["recommend"] ? row["recommend"].trim() : "",
    fuel_type: row["fuel_type"] ? row["fuel_type"].trim() : "",
    accident_status: row["accident_status"],
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
    verified: row["verified"],
    identity_front_view: row["identity_front_view"] ? row["identity_front_view"].trim() : "",
    identity_rear_view: row["identity_rear_view"] ? row["identity_rear_view"].trim() : "",
    license_frontal_view: row["license_frontal_view"] ? row["license_frontal_view"].trim() : "",
    license_rear_view: row["license_rear_view"] ? row["license_rear_view"].trim() : "",
  };
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
  };
}

function trim(str: string) {
  if (str) {
    return str.trim();
  } else {
    return null;
  }
}

processor.call("refresh", (ctx: ProcessorContext, domain: string, cbflag: string, vid?: string) => {
  log.info("refresh " + cbflag + " vid is " + vid);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const done = ctx.done;
  (async () => {
    try {
      // const dbVehicleModel = await db.query("SELECT vehicle_code, vehicle_name, brand_name, family_name, body_type, engine_desc, gearbox_name, year_pattern, group_name, cfg_level, purchase_price, purchase_price_tax, seat, effluent_standard, pl, fuel_jet_type, driven_type FROM vehicle_models");
      const dbVehicleModel = await db.query("SELECT vin, vehicles.vehicle_code AS vehicle_code, vehicle_name, brand_name, family_name, body_type, engine_desc, gearbox_name, year_pattern, group_name, cfg_level, purchase_price, purchase_price_tax, seat, effluent_standard, pl, fuel_jet_type, driven_type FROM vehicle_models, vehicles WHERE vehicles.vehicle_code = vehicle_models.vehicle_code" + (vid ? " AND vehicles.id = $1" : ""), (vid ? [vid] : []));
      const dbVehicle = await db.query("SELECT v.id AS id, uid, owner, owner_type, vehicle_code, license_no, engine_no, register_date, average_mileage, is_transfer, receipt_no, receipt_date, last_insurance_company, insurance_due_date, driving_frontal_view, driving_rear_view, recommend, fuel_type, accident_status, vin, v.created_at AS created_at, v.updated_at AS updated_at, p.id AS pid, name, identity_no, phone, verified, identity_frontal_view, identity_rear_view, license_frontal_view, license_rear_view FROM vehicles AS v, person AS p WHERE p.id = v.owner" + (vid ? " AND v.id = $1" : ""), (vid ? [vid] : []));
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
            vehicleCodeJson.push(vehicleCode);
            multi.sadd("vehicle-model", vin);
            let pkt2 = await msgpack_encode(vehicleModelJson);
            multi.hset("vehicle-vin-codes", vin, JSON.stringify(vehicleCodeJson));
            multi.hset("vehicle-model-entities", vehicleCode, pkt2);
            break;
          }
        }
      }
      let vehicleUsers: Object = {};
      console.log(vehicleJsons.length);
      for (let vehicle of vehicleJsons) {
        let vehicle_id = vehicle["id"];
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
          }
        }
        let pkt = await msgpack_encode(vehicle);
        multi.lpush("vehicle", vehicle_id);
        multi.hset("vehicle-entities", vehicle_id, pkt);
      }
      // for (const key of Object.keys(vehicleUsers)) {
      //   multi.lpush("vehicle-" + key, vehicleUsers[key]);
      // }
      await multi.execAsync();
      await set_for_response(cache, cbflag, { code: 200, data: "refresh success" });
      done();
      log.info("refresh success");
    } catch (e) {
      log.info(e);
      set_for_response(cache, cbflag, { code: 500, msg: e.message }).then(_ => {
        done();
      }).catch(e => {
        log.info(e);
        done();
      });
    }
  })();
});

processor.call("addVehicleModels", (ctx: ProcessorContext, vehicle_and_models: Object, cbflag: string) => {
  log.info(`addVehicleModels, vehicle_and_models: ${JSON.stringify(vehicle_and_models)}, cbflag: ${cbflag}`);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const done = ctx.done;
  (async () => {
    try {
      await db.query("BEGIN");
      const vehicle_models = vehicle_and_models["std_models"];
      for (const model of vehicle_models) {
        const dbmodel = await db.query("SELECT 1 FROM vehicle_models WHERE vehicle_code = $1", [model["vehicleCode"]]);
        if (dbmodel["rowCount"] === 0) {
          await db.query("INSERT INTO vehicle_models(vehicle_code, vehicle_name, brand_name, family_name, body_type, engine_desc, gearbox_name, year_pattern, group_name, cfg_level, purchase_price, purchase_price_tax, seat, effluent_standard, pl, fuel_jet_type, driven_type) VALUES($1, $2, $3, $4, $5, $6, $7, $8 ,$9, $10, $11, $12, $13, $14, $15, $16, $17)", [model["vehicleCode"], model["vehicleName"], model["brandName"], model["familyName"], model["bodyType"], model["engineDesc"], model["gearboxName"], model["yearPattern"], model["groupName"], model["cfgLevel"], model["purchasePrice"], model["purchasePriceTax"], model["seat"], model["effluentStandard"], model["pl"], model["fuelJetType"], model["drivenType"]]);
        }
      }
      await db.query("COMMIT");
      const multi = bluebird.promisifyAll(cache.multi()) as Multi;
      const codes = [];
      for (const model of vehicle_models) {
        const pkt = await msgpack_encode(model);
        multi.hset("vehicle-model-entities", model["vehicleCode"], pkt);
      }
      await multi.execAsync();
      const license = vehicle_and_models["licenseNo"];
      const pkt = await msgpack_encode(vehicle_and_models);
      await cache.hsetAsync("license-vehicle-models", license, pkt);
      await set_for_response(cache, cbflag, { code: 200, data: vehicle_and_models });
      done();
      log.info("addVehicleModels success");
    } catch (e) {
      log.error(e);
      await db.query("ROLLBACK");
      set_for_response(cache, cbflag, { code: 500, msg: e.message }).then(_ => {
        done();
      }).catch(e => {
        log.error(e);
        done();
      });
    }
  })();
});


processor.call("setPersonVerified", (ctx: ProcessorContext, identity_no: string, flag: boolean, callback: string) => {
  log.info("setPersonVerified " + identity_no);
  const db: PGClient = ctx.db;
  const cache: RedisClient = ctx.cache;
  const done = ctx.done;
  (async () => {
    try {
      let vids = [];
      await db.query("UPDATE person SET verified = $1 WHERE identity_no = $2 AND deleted = false", [flag, identity_no]);
      const vehicles = await db.query("SELECT id FROM vehicles WHERE owner in (SELECT id FROM person WHERE identity_no = $1)", [identity_no]);
      for (let row of vehicles["rows"]){
        let vehicleBuffer = await cache.hgetAsync("vehicle-entities", row["id"]);
        let vehicle = await msgpack_decode(vehicleBuffer);
        vehicle["owner"]["verified"] = flag;
        vehicleBuffer = await msgpack_encode(vehicle);
        await cache.hsetAsync("vehicle-entities", row["id"], vehicleBuffer);
        vids.push(row["id"]);
      }
      await set_for_response(cache, callback, { code: 200, data: vids });
      done();
    } catch (e) {
      log.error(e);
      set_for_response(cache, callback, { code: 500, msg: e.message }).then(_ => {
        done();
      }).catch(e => {
        log.error(e);
        done();
      });
    }
  })();
});

function sync_vehicle(db: PGClient, cache: RedisClient, vid: string) {
}
