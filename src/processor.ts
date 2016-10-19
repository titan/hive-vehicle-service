import { Processor, Config, ModuleFunction, DoneFunction, rpc, async_serial, async_serial_ignore } from "hive-processor";
import { Client as PGClient, ResultSet } from "pg";
import { createClient, RedisClient} from "redis";
import * as bunyan from "bunyan";
import { servermap, triggermap } from "hive-hostmap";
import * as uuid from "node-uuid";
import * as msgpack from "msgpack-lite";
import * as nanomsg from "nanomsg";
import * as http from "http";

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

// 新车已上牌个人
processor.call("setVehicleOnCard", (db: PGClient, cache: RedisClient, done: DoneFunction, pid: string, name: string, identity_no: string, phone: string, uid: string, recommend: string, vehicle_code: string, vid: string, license_no: string, engine_no: string,
  register_date: any, average_mileage: string, is_transfer: boolean, last_insurance_company: string, insurance_due_date: any, fuel_type: string) => {
  log.info("setVehicleInfoOnCard");
  // insert a record into person
  db.query("BEGIN", [], (err: Error) => {
    db.query("INSERT INTO person (id,name,identity_no,phone) VALUES ($1, $2, $3, $4)", [pid, name, identity_no, phone], (err: Error) => {
      if (err) {
        log.error(err, "query error");
        db.query("ROLLBACK", [], (err: Error) => {
          done();
        });
      } else {
        // insert a record into vehicle
        db.query("INSERT INTO vehicles (id, user_id, owner, owner_type, recommend, vehicle_code,license_no,engine_no,register_date,average_mileage,is_transfer, last_insurance_company,insurance_due_date, fuel_type) VALUES ($1, $2, $3, $4, $5, $6, $7, $8 ,$9, $10 ,$11, $12, $13, $14)", [vid, uid, pid, 0, recommend, vehicle_code, license_no, engine_no, register_date, average_mileage, is_transfer, last_insurance_company, insurance_due_date, fuel_type], (err: Error) => {
          if (err) {
            log.error(err, "query error");
            db.query("ROLLBACK", [], (err: Error) => {
              done();
            });
          } else {
            db.query("COMMIT", [], (err: Error) => {
              if (err) {
                log.error(err, "query error");
                done();
              } else {
                let vehicle = {
                  id: vid,
                  user_id: uid,
                  owner: {
                    id: pid,
                    name: name,
                    identity_no: identity_no,
                    phone: phone
                  },
                  owner_type: 0,
                  recommend: recommend,
                  drivers: [],
                  vehicle_code: vehicle_code,
                  license_no: license_no,
                  engine_no: engine_no,
                  register_date: register_date,
                  average_mileage: average_mileage,
                  is_transfer: is_transfer,
                  last_insurance_company: last_insurance_company,
                  insurance_due_date: insurance_due_date,
                  fuel_type: fuel_type
                };
                let multi = cache.multi();
                multi.hset("vehicle-entities", vid, JSON.stringify(vehicle));
                multi.lpush("vehicle", vid);
                multi.exec((err, replies) => {
                  if (err) {
                    log.error(err + "vehicle:" + vehicle);
                    done();
                  } else {
                    done();
                  }

                });
              }
            });
          }
        });
      }
    });
  });
});

// 新车未上牌个人
processor.call("setVehicle", (db: PGClient, cache: RedisClient, done: DoneFunction, pid: string, name: string, identity_no: string, phone: string, uid: string, recommend: string, vehicle_code: string, vid: string, engine_no: string, average_mileage: string, is_transfer: boolean, receipt_no: string, receipt_date: any, last_insurance_company: string, fuel_type: string) => {
  log.info("setVehicleInfo");
  db.query("BEGIN", [], (err: Error) => {
    // insert a record into person
    db.query("INSERT INTO person (id,name,identity_no,phone) VALUES ($1, $2, $3, $4)", [pid, name, identity_no, phone], (err: Error) => {
      if (err) {
        log.error(err, "query error");
        db.query("ROLLBACK", [], (err: Error) => {
          done();
        });
      } else {
        // insert a record into vehicle
        db.query("INSERT INTO vehicles (id, user_id, owner, owner_type, recommend, vehicle_code, engine_no,average_mileage,is_transfer,receipt_no, receipt_date,last_insurance_company, fuel_type) VALUES ($1, $2, $3, $4, $5, $6, $7, $8 ,$9, $10, $11, $12, $13)", [vid, uid, pid, 0, recommend, vehicle_code, engine_no, average_mileage, is_transfer, receipt_no, receipt_date, last_insurance_company, fuel_type], (err: Error) => {
          if (err) {
            log.error(err, "query error");
            db.query("ROLLBACK", [], (err: Error) => {
              done();
            });
          } else {
            db.query("COMMIT", [], (err: Error) => {
              if (err) {
                log.error(err, "query error");
                done();
              } else {
                let vehicle = {
                  id: vid,
                  user_id: uid,
                  owner: {
                    id: pid,
                    name: name,
                    identity_no: identity_no,
                    phone: phone
                  },
                  owner_type: 0,
                  recommend: recommend,
                  drivers: [],
                  vehicle_code: vehicle_code,
                  engine_no: engine_no,
                  license_no: null,
                  average_mileage: average_mileage,
                  is_transfer: is_transfer,
                  receipt_no: receipt_no,
                  receipt_date: receipt_date,
                  last_insurance_company: last_insurance_company,
                  fuel_type: fuel_type
                };
                let multi = cache.multi();
                multi.hset("vehicle-entities", vid, JSON.stringify(vehicle));
                multi.lpush("vehicle", vid);
                multi.exec((err, replies) => {
                  if (err) {
                    log.error(err);
                  }
                  done();
                });
              }
            });
          }
        });
      }
    });
  });
});

// 新车已上牌企业
// processor.call("setVehicleInfoOnCardEnterprise", (db: PGClient, cache: RedisClient, done: DoneFunction, args) => {
//   log.info({args: args}, "setVehicleInfoOnCardEnterprise");
//   db.query("BEGIN", [], (err: Error) => {
//     //insert a record into person
//     db.query("INSERT INTO enterprise_owner (id, name, society_code, contact_name, contact_phone) VALUES ($1, $2, $3, $4, $5)",[args.pid, args.name, args.society_code, args.contact_name, args.contact_phone], (err: Error) => {
//       if (err) {
//         log.error(err, "query error");
//         db.query("ROLLBACK", [], (err: Error) => {
//           done();
//         });
//       } else {
//         //insert a record into vehicle
//         db.query("INSERT INTO vehicles (id, user_id, owner, owner_type, recommend, vehicle_code,license_no,engine_no,register_date,average_mileage,is_transfer, last_insurance_company,insurance_due_date, fuel_type) VALUES ($1, $2, $3, $4, $5, $6, $7, $8 ,$9, $10 ,$11, $12, $13, $14)", [args.vid, args.uid, args.pid, 1, args.recommend, args.vehicle_code, args.license_no, args.engine_no, args.register_date, args.average_mileage, args.is_transfer,args.last_insurance_company, args.insurance_due_date, args.fuel_type], (err: Error) => {
//           if (err) {
//             log.error(err, "query error");
//             db.query("ROLLBACK", [], (err: Error) => {
//               done();
//             });
//           } else {
//             db.query("COMMIT", [], (err: Error) => {
//               if (err) {
//                 log.error(err, "query error");
//                 done();
//               } else {
//                 let vehicle = {
//                   id: args.vid,
//                   user_id: args.uid,
//                   owner: {
//                     id: args.pid,
//                     name: args.name,
//                     identity_no: args.identity_no,
//                     phone: args.phone
//                   },
//                   owner_type: 1,
//                   recommend: args.recommend,
//                   drivers: [],
//                   vehicle_code: args.vehicle_code,
//                   license_no: args.license_no,
//                   engine_no: args.engine_no,
//                   register_date: args.register_date,
//                   average_mileage: args.average_mileage,
//                   is_transfer: args.is_transfer,
//                   last_insurance_company: args.last_insurance_company,
//                   insurance_due_date: args.insurance_due_date,
//                   fuel_type:args.fuel_type
//                 };
//                 let multi = cache.multi();
//                 multi.hset("vehicle-entities", args.vid, JSON.stringify(vehicle));
//                 multi.lpush("vehicle", args.vid);
//                 multi.exec((err, replies) => {
//                   if (err) {
//                     log.error(err);
//                   }
//                   done();
//                 });
//               }
//             });
//           }
//         });
//       }
//     });
//   });
// });
// 新车未上牌企业
// processor.call("setVehicleInfoEnterprise", (db: PGClient, cache: RedisClient, done: DoneFunction, args) => {
//   log.info({args: args}, "setVehicleInfoEnterprise");
//   db.query("BEGIN", [], (err: Error) => {
//     //insert a record into person
//     db.query("INSERT INTO enterprise_owner (id, name, society_code, contact_name, contact_phone) VALUES ($1, $2, $3, $4, $5)",[args.pid, args.name, args.society_code, args.contact_name, args.contact_phone], (err: Error) => {
//       if (err) {
//         log.error(err, "query error");
//         db.query("ROLLBACK", [], (err: Error) => {
//           done();
//         });
//       } else {
//         //insert a record into vehicle
//         db.query("INSERT INTO vehicles (id, user_id, owner, owner_type, recommend, vehicle_code, engine_no,average_mileage,is_transfer,receipt_no, receipt_date,last_insurance_company,fuel_type) VALUES ($1, $2, $3, $4, $5, $6, $7, $8 ,$9, $10, $11, $12, $13)",[args.vid, args.uid, args.pid, 1, args.recommend, args.vehicle_code, args.engine_no, args.average_mileage, args.is_transfer, args.receipt_no, args.receipt_date, args.last_insurance_company, args.fuel_type], (err: Error) => {
//           if (err) {
//             log.error(err, "query error");
//             db.query("ROLLBACK", [], (err: Error) => {
//               done();
//             });
//           } else {
//             db.query("COMMIT", [], (err: Error) => {
//               if (err) {
//                 log.error(err, "query error");
//                 done();
//               } else {
//                 let vehicle = {
//                   id: args.vid,
//                   user_id: args.uid,
//                   owner: {
//                     id: args.pid,
//                     name: args.name,
//                     identity_no: args.identity_no,
//                     phone: args.phone
//                   },
//                   owner_type: 1,
//                   recommend: args.recommend,
//                   drivers: [],
//                   vehicle_code: args.vehicle_code,
//                   engine_no: args.engine_no,
//                   average_mileage: args.average_mileage,
//                   is_transfer: args.is_transfer,
//                   receipt_no: args.receipt_no,
//                   receipt_date: args.receipt_date,
//                   last_insurance_company: args.last_insurance_company,
//                   fuel_type: args.fuel_type
//                 };
//                 let multi = cache.multi();
//                 multi.hset("vehicle-entities", args.vid, JSON.stringify(vehicle));
//                 multi.lpush("vehicle", args.vid);
//                 multi.exec((err, replies) => {
//                   if (err) {
//                     log.error(err);
//                   }
//                   done();
//                 });
//               }
//             });
//           }
//         });
//       }
//     });
//   });
// });

interface InsertDriverCtx {
  pids: any;
  dids: any;
  vid: string;
  cache: RedisClient;
  db: PGClient;
  done: DoneFunction;
}

function insert_person_recur(ctx: InsertDriverCtx, persons: Object[]) {
  if (persons.length === 0) {
    ctx.done();
  } else {
    let person = persons.shift();
    ctx.db.query("BEGIN", [], (err: Error) => {
      let pid = ctx.pids.shift();
      ctx.db.query("INSERT INTO person (id, name, identity_no,phone) VALUES ($1, $2, $3, $4)", [pid, person["name"], person["identity"], person["phone"]], (err: Error) => {
        if (err) {
          log.error(err, "query error");
          ctx.db.query("ROLLBACK", [], (err: Error) => {
            insert_person_recur(ctx, []);
          });
        } else {
          let did = ctx.dids.shift();
          ctx.db.query("INSERT INTO drivers (id, vid, pid, is_primary) VALUES ($1, $2, $3, $4)", [did, ctx.vid, pid, person["is_primary"]], (err: Error) => {
            if (err) {
              log.error(err, "query error");
              ctx.db.query("ROLLBACK", [], (err: Error) => {
                insert_person_recur(ctx, []);
              });
            } else {
              ctx.db.query("COMMIT", [], (err: Error) => {
                if (err) {
                  log.error(err, "query error");
                  insert_person_recur(ctx, []);
                } else {
                  ctx.cache.hget("vehicle-entities", ctx.vid, (err, result) => {
                    if (result) {
                      let vehicle = JSON.parse(result);
                      vehicle["drivers"].push({
                        id: pid,
                        name: person["name"],
                        identity_no: person["identity"],
                        phone: person["phone"],
                        is_primary: person["is_primary"]
                      });
                      ctx.cache.hset("vehicle-entities", ctx.vid, JSON.stringify(vehicle), (err, result) => {
                        insert_person_recur(ctx, persons);
                      });
                    } else {
                      insert_person_recur(ctx, persons);
                    }
                  });
                }
              });
            }
          });
        }
      });
    });
  }
}

processor.call("setDriver", (db: PGClient, cache: RedisClient, done: DoneFunction, pids: any, dids: any, vid: string, drivers: any) => {
  log.info("setDriverInfo");
  let ctx = {
    db,
    cache,
    done,
    pids: pids,
    dids: dids,
    vid: vid
  };

  insert_person_recur(ctx, drivers);
});

// processor.call("changeDriverInfo", (db: PGClient, cache: RedisClient, done: DoneFunction, args) => {
//   log.info({args: args}, "changeDriverInfo");
//   db.query("UPDATE person SET name=$1, identity_no=$2, phone=$3 WHERE id=$4", [args.name, args.identity_no, args.phone, args.pid], (err: Error) => {
//     if (err) {
//       log.error(err);
//       done();
//     } else {
//       let multi = cache.multi();
//       let vehicle = multi.hget("vehicle-entities",args.vid);
//       let drivers = vehicle["drivers"];
//       let new_drivers = [];
//       for (let driver of drivers) {
//         if (driver.id != args.pid) {
//           new_drivers.push(driver);
//         }
//       }
//       new_drivers.push({
//         id: args.pid,
//         name: name,
//         identity_no: args.identity_no,
//         phone: args.phone
//       });
//       vehicle["drivers"] = new_drivers;
//       multi.exec((err, replies) => {
//         if (err) {
//           log.error(err);
//         }
//         done();
//       });
//     }
//   });
// });

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
        }));
        done();
      } else if (vehiclejson) {
        const vehicle = JSON.parse(vehiclejson);
        vehicle["driving_frontal_view"] = driving_frontal_view;
        vehicle["driving_rear_view"] = driving_rear_view;
        cache.hset("vehicle-entities", vid, JSON.stringify(vehicle), (err1, reply) => {
          if (err1) {
            cache.setex(callback, 30, JSON.stringify({
              code: 500,
              msg: err1.message
            }));
          } else {
            cache.setex(callback, 30, JSON.stringify({
              code: 200,
              msg: "Success"
            }));
          }
          done();
        });
      } else {
        cache.setex(callback, 30, JSON.stringify({
          code: 404,
          msg: "Vehicle not found"
        }));
        done();
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
      }));
      done();
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

function insert_vehicle_model_recur(ctx: InsertModelCtx, models: Object[]) {
  if (models.length === 0) {
    let multi = ctx.cache.multi();
    let codes = [];
    for (let model of ctx.models) {
      model["vin_code"] = ctx.vin;
      multi.hset("vehicle-model-entities", model["vehicleCode"], JSON.stringify(model));
      codes.push(model["vehicleCode"]);
    }
    multi.hset("vehicle-vin-codes", ctx.vin, JSON.stringify(codes));
    multi.sadd("vehicle-model", ctx.vin);
    multi.exec((err, replies) => {
      if (err) {
        log.error(err);
      }
      ctx.done(); // close db and cache connection
    });
  } else {
    let model = models.shift();
    ctx.db.query("INSERT INTO vehicle_model(vehicle_code, vin_code, vehicle_name, brand_name, family_name, body_type, engine_number, engine_desc, gearbox_name, year_pattern, group_name, cfg_level, purchase_price, purchase_price_tax, seat, effluent_standard, pl, fuel_jet_type, driven_type) VALUES($1, $2, $3, $4, $5, $6, $7, $8 ,$9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)", [model["vehicleCode"], ctx.vin, model["vehicleName"], model["brandName"], model["familyName"], model["bodyType"], model["engineNumber"], model["engineDesc"], model["gearboxName"], model["yearPattern"], model["groupName"], model["cfgLevel"], model["purchasePrice"], model["purchasePriceTax"], model["seat"], model["effluentStandard"], model["pl"], model["fuelJetType"], model["drivenType"]], (err: Error) => {
      if (err) {
        log.error(err, "query error");
      }
      insert_vehicle_model_recur(ctx, models);
    });
  }
}

processor.call("getVehicleModelsByMake", (db: PGClient, cache: RedisClient, done: DoneFunction, args2: any, vin: string) => {
  log.info("getVehicleModelsByMake");
  let ctx = {
    db,
    cache,
    done,
    vin: vin,
    models: args2.vehicleList.map(e => e)
  };
  insert_vehicle_model_recur(ctx, args2.vehicleList);
});

function row2model(row: Object) {
  return {
    vehicleCode: row["vehicle_code"] ? row["vehicle_code"].trim() : "",
    vin_code: row["vin_code"] ? row["vin_code"].trim() : "",
    vehicleName: row["vehicle_name"] ? row["vehicle_name"].trim() : "",
    brandName: row["brand_name"] ? row["brand_name"].trim() : "",
    familyName: row["family_name"] ? row["family_name"].trim() : "",
    bodyType: row["body_type"] ? row["body_type"].trim() : "",
    engineNumber: row["engineNumber"] ? row["engineNumber"].trim() : "",
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
    db.query("SELECT v.id AS v_id, v.user_id AS v_user_id, v.owner AS v_owner, v.owner_type AS v_owner_type , v.vehicle_code AS v_vehicle_code, v.license_no AS v_license_no,  v.engine_no AS v_engine_no, v.register_date AS v_registeer_date, v.average_mileage AS v_average_mileage, v.is_transfer AS v_is_transfer, v.receipt_no AS v_receipt_no, v.receipt_date AS v_receipt_data, v.last_insurance_company AS v_last_insurance_company, v.insurance_due_date AS v_insurance_due_date, v.driving_frontal_view AS v_driving_frontal_view, v.driving_rear_view AS v_driving_rear_view, v.created_at AS v_created_at, v.updated_at AS v_updated_at, v.recommend AS v_recommend, v.fuel_type AS v_fuel_type, d.pid AS d_pid , m.vehicle_code AS m_vehicle_code, m.vin_code AS m_vin_code, m.vehicle_name AS m_vehicle_name, m.brand_name AS m_brand_name, m.family_name AS m_family_name, m.body_type AS m_body_type, m.engine_number AS m_engine_number, m.engine_desc AS m_engine_desc, m.gearbox_name AS m_gearbox_name, m.year_pattern AS m_year_pattern, m.group_name AS m_group_name, m.cfg_level AS m_cfg_level, m.purchase_price AS m_purchase_price, m.purchase_price_tax AS m_purchase_price_tax, m.seat AS m_seat, m.effluent_standard AS m_effluent_standard, m.pl AS m_pl, m.fuel_jet_type AS m_fuel_jet_type, m.driven_type AS m_driven_type,p.id AS p_id, p.name AS p_name, p.identity_no AS p_identity, p.phone AS p_phone, p.identity_frontal_view AS p_identity_frontal_view, p.identity_rear_view AS p_identity_rear_view, p.license_frontal_view AS p_license_frontal_view, p.license_rear_view AS p_license_rear_view FROM vehicles AS v LEFT JOIN drivers AS d ON v.id = d.vid LEFT JOIN vehicle_model AS m ON v.vehicle_code = m.vehicle_code LEFT JOIN person AS p ON d.pid = p.id", [], (e: Error, result: ResultSet) => {
      if (e) {
        reject(e);
        log.info("err : SELECT query error" + e);
      } else {
        const vehicles: Object = {};
        for (const row of result.rows) {
          if (vehicles.hasOwnProperty(row.v_id)) {
            vehicles[row.v_id]["pids"].push(row.d_pid);
          } else {
            const vehicle = {
              id: row.v_id,
              user_id: row.v_user_id,
              owid: row.v_owner,
              owner: {},
              owner_type: trim(row.v_owner_type),
              vehicle_code: trim(row.v_vehicle_code),
              license_no: trim(row.v_license_no),
              engine_no: trim(row.v_engine_no),
              register_date: row.v_receipt_data,
              average_mileage: trim(row.v_average_mileage),
              drivers: [],
              pids: [row.d_pid],
              vehicle_model: {
                vehicle_code: trim(row.m_vehicle_code),
                vin_code: trim(row.m_vin_code),
                vehicle_name: trim(row.m_vehicle_name),
                brand_name: trim(row.m_brand_name),
                family_name: trim(row.m_family_name),
                body_type: trim(row.m_body_type),
                engine_number: trim(row.m_engine_number),
                engine_desc: trim(row.m_engine_desc),
                gearbox_name: trim(row.m_gearbox_name),
                year_pattern: trim(row.m_year_pattern),
                group_name: trim(row.m_group_name),
                cfg_level: trim(row.m_cfg_level),
                purchase_price: row.m_purchase_price,
                purchase_price_tax: row.m_purchase_price_tax,
                seat: row.m_seat,
                effluent_standard: row.m_effluent_standard,
                pl: row.m_pl,
                fuel_jet_type: row.m_fuel_jet_type,
                driven_type: row.m_driven_type
              },
              is_transfer: row.v_is_transfer,
              receipt_no: row.v_receipt_no,
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
        for (let vid of vids) {
          for (let pid of vehicles[vid]["pids"]) {
            if (pid !== null) {
              for (let row of result.rows) {
                if (pid = row.p_id) {
                  vehicles[vid]["drivers"].push({
                    id: row.p_id,
                    name: trim(row.p_name),
                    identity_no: trim(row.p_identity_no),
                    phone: row.p_phone,
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
          for (let driver of vehicles[vid]["drivers"]) {
            if (driver != null) {
              if (vehicles[vid]["owid"] == driver["id"]) {
                vehicles[vid]["owner"]["id"] === driver["id"];
                vehicles[vid]["owner"]["name"] === driver["name"];
                vehicles[vid]["owner"]["identity_no"] === driver["identity_no"];
                vehicles[vid]["owner"]["phone"] === driver["phone"];
                vehicles[vid]["owner"]["identity_front_view"] === driver["identity_front_view"];
                vehicles[vid]["owner"]["identity_rear_view"] === driver["identity_rear_view"];
                vehicles[vid]["owner"]["license_frontal_view"] === driver["license_frontal_view"];
                vehicles[vid]["owner"]["license_rear_view"] === driver["license_rear_view"];
              }
            }
          }
        }
        const multi = cache.multi();
        for (const vid of Object.keys(vehicles)) {
          const vehicle = vehicles[vid];
          delete vehicle["pids"];
          delete vehicle["owid"];
          log.info(JSON.stringify(vehicle) + "vehicle==========")
          multi.hset("vehicle-entities", vid, JSON.stringify(vehicle));
          multi.lpush("vehicle", vid);
          multi.hset("vehicle-vin-codes", vehicle["vehicle_model"]["vin_code"], JSON.stringify(vehicle["vehicle_model"]));
          multi.sadd("vehicle-model", vehicle["vehicle_model"]["vin_code"]);
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

// processor.call("refresh", (db: PGClient, cache: RedisClient, done: DoneFunction) => {
//   log.info("refresh");
//   db.query("SELECT vehicle_code, vin_code, vehicle_name, brand_name, family_name, body_type, engine_number, engine_desc, gearbox_name, year_pattern, group_name, cfg_level, purchase_price, purchase_price_tax, seat, effluent_standard, pl, fuel_jet_type, driven_type FROM vehicle_model", [], (err: Error, result) => {
//     if (err) {
//       log.error(err, "query error");
//       done();
//     } else {
//       let models = [];
//       for (let row of result.rows) {
//         models.push(row2model(row));
//       }
//       let vins = {};
//       for (let row of result.rows) {
//         if (vins.hasOwnProperty(row.vin_code)) {
//           vins[row.vin_code].push(row.vehicle_code);
//         } else {
//           vins[row.vin_code] = [row.vehicle_code];
//         }
//       }
//       let multi = cache.multi();
//       for (let model of models) {
//         multi.hset("vehicle-model-entities", model["vehicleCode"], JSON.stringify(model));
//       }
//       for (let vin in vins) {
//         if (vins.hasOwnProperty(vin)) {
//           multi.hset("vehicle-vin-codes", vin, JSON.stringify(vins[vin]));
//           multi.sadd("vehicle-model", vin);
//         }
//       }
//       multi.exec((err, replies) => {
//         if (err) {
//           log.error("multi err" + err);
//           done();
//         } else {
//           db.query("SELECT id, user_id, owner, owner_type, vehicle_code, license_no, engine_no, register_date, average_mileage, is_transfer, receipt_no, receipt_date, last_insurance_company, insurance_due_date, driving_frontal_view, driving_real_view, created_at, updated_at, recommend, fuel_type FROM vehicles", [], (err2: Error, result2) => {
//             if (err2) {
//               log.error(err2, "query error");
//               done();
//             } else {
//               db.query("SELECT id, name, identity_no, phone, identity_front_view, identity_rear_view, license_frontal_view, license_rear_view, created_at, updated_at FROM person", [], (err3: Error, result3) => {
//                 if (err3) {
//                   log.error(err3, "query error");
//                   done();
//                 } else {
//                   db.query("SELECT id, pid, is_primary, created_at, updated_at FROM drivers where vid = $1", [result2.row["id"]], (err4: Error, result4) => {
//                     if (err4) {
//                       log.error(err4, "query error");
//                       done();
//                     } else {
//                       let vehicles = [];
//                       for (let row of result2.rows) {
//                         vehicles.push(row2vehicle(row));
//                       }
//                       for (let row2 of result3.rows) {

//                       }
//                       for (let row3 of result4.rows) {

//                       }
//                       let multi = cache.multi();
//                       for (let vehicle of vehicles) {

//                       }
//                     }
//                   });
//                 }
//               });
//             }
//           });
//         }
//       });
//     }
//   });
// });

log.info("Start processor at %s", config.addr);

processor.run();

