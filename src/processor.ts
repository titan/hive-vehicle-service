import { Processor, Config, ModuleFunction, DoneFunction } from 'hive-processor';
import { Client as PGClient, ResultSet } from 'pg';
import { createClient, RedisClient} from 'redis';
import * as bunyan from 'bunyan';
import * as hostmap from './hostmap'

let log = bunyan.createLogger({
  name: 'vehicle-processor',
  streams: [
    {
      level: 'info',
      path: '/var/log/vehicle-processor-info.log',  // log ERROR and above to a file
      type: 'rotating-file',
      period: '1d',   // daily rotation
      count: 7        // keep 7 back copies
    },
    {
      level: 'error',
      path: '/var/log/vehicle-processor-error.log',  // log ERROR and above to a file
      type: 'rotating-file',
      period: '1w',   // daily rotation
      count: 3        // keep 7 back copies
    }
  ]
});

let config: Config = {
  dbhost: process.env['DB_HOST'],
  dbuser: process.env['DB_USER'],
  dbport: process.env['DB_PORT'],
  database: process.env['DB_NAME'],
  dbpasswd: process.env['DB_PASSWORD'],
  cachehost: process.env['CACHE_HOST'],
  addr: "ipc:///tmp/vehicle.ipc"
};

let processor = new Processor(config);

//新车已上牌个人
processor.call('setVehicleInfoOnCard', (db: PGClient, cache: RedisClient, done: DoneFunction, args) => {
  log.info('setVehicleInfoOnCard');
  //insert a record into person 
  db.query('INSERT INTO person (id,name,identity_no,phone) VALUES ($1, $2, $3, $4, $5, $6)',[args.pid, args.name, args.identity_no, args.phone, 0], (err: Error) => {
     if (err) {
      log.error(err, 'query error');
     }else{
        //insert a record into vehicle
        db.query('INSERT INTO vehicles (id, user_id, owner, owner_type, vehicle_code,license_no,engine_no,register_date,average_mileage,is_transfer,\
        last_insurance_company,insurance_due_date) VALUES ($1, $2, $3, $4, $5, $6, $7, $8 ,$9, $10 ,$11, $12)',[args.vid, args.uid, args.pid, 0, args.vehicle_code, args.license_no, args.engine_no, args.register_date,
         args.average_mileage, args.is_transfer,args.last_insurance_company, args.insurance_due_date], (err: Error) => {
          if (err) {
            log.error(err, 'query error');
          }else{
            let vehicle = {id:args.vid, owenr:{id:args.pid, user_id:args.uid, name:name, identity_no:args.identity_no, phone:args.phone}, owner_type:0, drivers:[], vehicle_code:args.vehicle_code, license_no:args.license_no, 
            engine_no:args.engine_no, register_date:args.register_date, average_mileage:args.average_mileage, is_transfer:args.is_transfer,last_insurance_company:args.last_insurance_company, insurance_due_date:args.insurance_due_date};
            let multi = cache.multi();
            multi.hset("vehicle-entities", args.vid, JSON.stringify(vehicle));
            multi.lpush("vehicle", args.vid);
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
});
//新车未上牌个人
processor.call('setVehicleInfo', (db: PGClient, cache: RedisClient, done: DoneFunction, args) => {
  log.info('setVehicleInfo');
  //insert a record into person 
  db.query('INSERT INTO person (id,name,identity_no,phone) VALUES ($1, $2, $3, $4, $5)',[args.pid, args.name, args.identity_no, args.phone, 0], (err: Error) => {
     if (err) {
      log.error(err, 'query error');
     }else{
        //insert a record into vehicle
        db.query('INSERT INTO vehicles (id, user_id, owner, owner_type, vehicle_code,license_no,engine_no,average_mileage,is_transfer,receipt_no,\
        receipt_date,last_insurance_company) VALUES ($1, $2, $3, $4, $5, $6, $7, $8 ,$9, $10, $11, $12)',[args.vid, args.uid,
        args.pid, 1, args.vehicle_code, args.license_no, args.engine_no, args.average_mileage, args.is_transfer, args.receipt_no, args.receipt_date, args.last_insurance_company], (err: Error) => {
          if (err) {
            log.error(err, 'query error');
          }else{
            let vehicle = {id:args.vid, owner:{id:args.pid, user_id:args.uid, name:args.name, identity_no:args.identity_no, phone:args.phone}, owner_type:0, drivers:[], vehicle_code:args.vehicle_code, license_no:args.license_no, 
            engine_no:args.engine_no, average_mileage:args.average_mileage, is_transfer:args.is_transfer, receipt_no:args.receipt_no, receipt_date:args.receipt_date, last_insurance_company:args.last_insurance_company};
            let multi = cache.multi();
            multi.hset("vehicle-entities", args.vid, JSON.stringify(vehicle));
            multi.lpush("vehicle", args.vid);
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
});

//新车已上牌企业
processor.call('setVehicleInfoOnCardEnterprise', (db: PGClient, cache: RedisClient, done: DoneFunction, args) => {
  log.info('setVehicleInfoOnCard');
  //insert a record into person 
  db.query('INSERT INTO enterprise_owner (id, name, society_code, contact_name, contact_phone) VALUES ($1, $2, $3, $4, $5)',[args.pid, args.name, args.society_code, args.contact_name, args.contact_phone], (err: Error) => {
     if (err) {
      log.error(err, 'query error');
     }else{
        //insert a record into vehicle
        db.query('INSERT INTO vehicles (id, user_id, owner, owner_type, vehicle_code,license_no,engine_no,register_date,average_mileage,is_transfer,\
        last_insurance_company,insurance_due_date) VALUES ($1, $2, $3, $4, $5, $6, $7, $8 ,$9, $10 ,$11, $12)',[args.vid, args.uid, args.pid, 1, args.vehicle_code, args.license_no, args.engine_no, args.register_date,
         args.average_mileage, args.is_transfer,args.last_insurance_company, args.insurance_due_date], (err: Error) => {
          if (err) {
            log.error(err, 'query error');
          }else{
            let vehicle = {id:args.vid, owenr:{id:args.pid, user_id:args.uid, name:name, identity_no:args.identity_no, phone:args.phone}, owner_type:1, drivers:[], vehicle_code:args.vehicle_code, license_no:args.license_no, 
            engine_no:args.engine_no, register_date:args.register_date, average_mileage:args.average_mileage, is_transfer:args.is_transfer,last_insurance_company:args.last_insurance_company, insurance_due_date:args.insurance_due_date};
            let multi = cache.multi();
            multi.hset("vehicle-entities", args.vid, JSON.stringify(vehicle));
            multi.lpush("vehicle", args.vid);
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
});
//新车未上牌企业
processor.call('setVehicleInfoEnterprise', (db: PGClient, cache: RedisClient, done: DoneFunction, args) => {
  log.info('setVehicleInfo');
  //insert a record into person 
  db.query('INSERT INTO enterprise_owner (id, name, society_code, contact_name, contact_phone) VALUES ($1, $2, $3, $4, $5)',[args.pid, args.name, args.society_code, args.contact_name, args.contact_phone], (err: Error) => {
     if (err) {
      log.error(err, 'query error');
     }else{
        //insert a record into vehicle
        db.query('INSERT INTO vehicles (id, user_id, owner, owner_type, vehicle_code,license_no,engine_no,average_mileage,is_transfer,receipt_no,\
        receipt_date,last_insurance_company) VALUES ($1, $2, $3, $4, $5, $6, $7, $8 ,$9, $10, $11, $12)',[args.vid, args.uid,
        args.pid, 1, args.vehicle_code, args.license_no, args.engine_no, args.average_mileage, args.is_transfer, args.receipt_no, args.receipt_date, args.last_insurance_company], (err: Error) => {
          if (err) {
            log.error(err, 'query error');
          }else{
            let vehicle = {id:args.vid, owner:{id:args.pid, user_id:args.uid, name:args.name, identity_no:args.identity_no, phone:args.phone}, owner_type:1, drivers:[], vehicle_code:args.vehicle_code, license_no:args.license_no, 
            engine_no:args.engine_no, average_mileage:args.average_mileage, is_transfer:args.is_transfer, receipt_no:args.receipt_no, receipt_date:args.receipt_date, last_insurance_company:args.last_insurance_company};
            let multi = cache.multi();
            multi.hset("vehicle-entities", args.vid, JSON.stringify(vehicle));
            multi.lpush("vehicle", args.vid);
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
});

processor.call('setDriverInfo', (db: PGClient, cache: RedisClient, done: DoneFunction, args) => {
  log.info('setDriverInfo');
  //insert a record into person 
  for(let driver of args.drivers){
    db.query('INSERT INTO person (id,name,identity_no,phone) VALUES ($1, $2, $3, $4, $5)',[args.pid, driver.name, driver.identity_no, driver.phone], (err: Error) => {
      if (err) {
        log.error(err, 'query error');
      }else{
          db.query('INSERT INTO drivers (id, vid, pid, is_primary) VALUES ($1, $2, $3, $4)',[args.did, args.vid, args.pid, driver.is_primary], 
          (err: Error) => {
            if (err) {
              log.error(err, 'query error');
            }else{
              let multi = cache.multi();
              let vehicle=multi.hget("vehicle-entities", args.vid);
              vehicle["drivers"].push({id:args.pid, name:driver.name, identity_no:driver.identity_no ,phone:driver.phone});
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

processor.call('changeDriverInfo', (db: PGClient, cache: RedisClient, done: DoneFunction, args) => {
  log.info('changeDriverInfo');
  db.query('UPDATE person SET name=$1, identity_no=$2, phone=$3 WHERE id=$4',[args.name, args.identity_no, args.phone, args.pid],(err: Error) => {
      if (err) {
        log.error(err);
      }else{
        let multi = cache.multi();
        let vehicle = multi.hget("vehicle-entities",args.vid);
        let drivers = vehicle["drivers"];
        let new_drivers = [];
        for(let driver of drivers){
          if(driver.id != args.pid){
            new_drivers.push(driver);
          }
        }
        new_drivers.push({id:args.pid, name:name,identity_no:args.identity_no, phone:args.phone});
        vehicle["drivers"] = new_drivers;
        multi.exec((err, replies) => {
          if (err) {
            log.error(err);
          }
          done();
        });
      }
  });
});

processor.call('uploadDriverImages', (db: PGClient, cache: RedisClient, done: DoneFunction, args) => {
  log.info('uploadDriverImages');
  db.query('UPDATE vehicles SET driving_frontal_view=$1, driving_rear_view=$2 WHERE id=$3',[args.driving_frontal_view, args.driving_rear_view, args.vid],(err: Error) => {
      if (err) {
        log.error(err);
      }else{
        db.query('UPDATE person SET identity_frontal_view=$1, identity_rear_view=$2 WHERE id in (SELECT owner FROM vehicles WHERE id = $3)',[args.identity_frontal_view, args.identity_rear_view, args.vid],(err: Error) => {
          if (err) {
            log.error(err);
          }else{
            let pids = [];
            let images = [];
            let obj = null;
            for(let key in args.license_frontal_views){
              if(key){
                images.push(obj[key]);
                pids.push(key);
              }
            }
            for(let i=0; i<pids.length; i++){
              db.query('UPDATE person SET license_frontal_view=$1 WHERE id = $2)',[images[i], pids[i]],(err: Error) => {
                if (err) {
                  log.error(err);
                }
              });
            }
            let multi = cache.multi();
            let vehicle=multi.hget("vehicle-entities", args.vid);
            vehicle["driving_frontal_view"] = args.driving_frontal_view;
            vehicle["driving_rear_view"] = args.driving_rear_view;
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
});

processor.call('getVehicleModelsByMake', (db: PGClient, cache: RedisClient, done: DoneFunction, args) => {
  log.info('getVehicleModelsByMake');
  let vin = args[1];
  let countdown = args[0].vehicleList.length;
  for(let arg of args[0].vehicleList){
    db.query('INSERT INTO vehicle_model(vehicle_code,vin_code,vehicle_name,brand_name,family_name,body_type,engine_number,engine_desc,gearbox_name,year_pattern,\
    group_name,cfg_level,purchase_price,purchase_price_tax,seat,effluent_standard,pl,fuel_jet_type,driven_type) VALUES($1, $2, $3, $4, $5, $6, $7, $8 ,$9, $10,\
    $11, $12, $13, $14, $15, $16, $17, $18, $19)', [arg.vehicleCode, vin, arg.vehicleName,arg.brandName,arg.familyName,arg.bodyType,
     arg.engineNumber, arg.engineDesc, arg.gearboxName, arg.yearPattern, arg.groupName, arg.cfgLevel, arg.purchasePrice, arg.purchasePriceTax, arg.seat,
    arg.effluentStandard, arg.pl, arg.fuelJetType, arg.drivenType], (err: Error) => {
      if (err) {
        log.error(err, 'query error');
      }
      countdown--;
      if (countdown == 0) {
        let multi = cache.multi();
        multi.hset("vehicle-model-entities", vin, JSON.stringify(args[0].vehicleList));
        multi.sadd("vehicle-model", vin);
        multi.exec((err, replies) => {
          if (err) {
            log.error(err);
          }
          done(); // close db and cache connection
        });
      }
    });
 }
});


log.info('Start processor at %s', config.addr);

processor.run();

