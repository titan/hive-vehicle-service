import { Processor, Config, ModuleFunction, DoneFunction } from 'hive-processor';
import { Client as PGClient, ResultSet } from 'pg';
import { createClient, RedisClient} from 'redis';
import * as bunyan from 'bunyan';

let log = bunyan.createLogger({
  name: 'vehicle-processor',
  streams: [
    {
      level: 'info',
      path: '/var/log/processor-info.log',  // log ERROR and above to a file
      type: 'rotating-file',
      period: '1d',   // daily rotation
      count: 7        // keep 7 back copies
    },
    {
      level: 'error',
      path: '/var/log/processor-error.log',  // log ERROR and above to a file
      type: 'rotating-file',
      period: '1w',   // daily rotation
      count: 3        // keep 7 back copies
    }
  ]
});

let config: Config = {
  dbhost: process.env['DB_HOST'],
  dbuser: process.env['DB_USER'],
  database: process.env['DB_NAME'],
  dbpasswd: process.env['DB_PASSWORD'],
  cachehost: process.env['CACHE_HOST'],
  addr: "ipc:///tmp/queue.ipc"
};

let processor = new Processor(config);

//新车已上牌
processor.call('setVehicleInfoOnCard', (db: PGClient, cache: RedisClient, done: DoneFunction, pid, name,  identity_no, phone, user_id, vehicle_code,vid,license_no, engine_no, 
  register_date, average_mileage, is_transfer,last_insurance_company, insurance_due_date) => {
  log.info('setVehicleInfoOnCard');
  //insert a record into person 
  db.query('INSERT INTO person (id,name,identity_no,phone) VALUES ($1, $2, $3, $4, $5)',[pid,
   name,  identity_no, phone], (err: Error) => {
     if (err) {
      log.error(err, 'query error');
     }else{
        //insert a record into vehicle
        db.query('INSERT INTO vehicles (id, user_id, owner,vehicle_code,license_no,engine_no,register_date,average_mileage,is_transfer,\
        last_insurance_company,insurance_due_date) VALUES ($1, $2, $3, $4, $5, $6, $7, $8 ,$9, $10 ,$11)',[vid, user_id,pid, vehicle_code, license_no, engine_no, register_date,
         average_mileage, is_transfer,last_insurance_company, insurance_due_date], (err: Error) => {
          if (err) {
            log.error(err, 'query error');
          }else{
            let vehicle = {id:vid, owenr:{id:pid, user_id:user_id, name:name, identity_no:identity_no,phone:phone}, drivers:[], vehicle_code:vehicle_code, license_no:license_no, 
            engine_no:engine_no, register_date:register_date, average_mileage:average_mileage, is_transfer:is_transfer,last_insurance_company:last_insurance_company, insurance_due_date:insurance_due_date};
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
});
//新车未上牌
processor.call('setVehicleInfo', (db: PGClient, cache: RedisClient, done: DoneFunction, pid, name, identity_no, phone, user_id, vehicle_code, vid, license_no, engine_no, average_mileage, is_transfer,
   receipt_no, receipt_date, last_insurance_company, insurance_due_date) => {
  log.info('setVehicleInfo');
  //insert a record into person 
  db.query('INSERT INTO person (id,name,identity_no,phone) VALUES ($1, $2, $3, $4)',[pid,name,  identity_no, phone], (err: Error) => {
     if (err) {
      log.error(err, 'query error');
     }else{
        //insert a record into vehicle
        db.query('INSERT INTO vehicles (id, user_id, owner,vehicle_code,license_no,engine_no,average_mileage,is_transfer,receipt_no,\
        receipt_date,last_insurance_company,insurance_due_date) VALUES ($1, $2, $3, $4, $5, $6, $7, $8 ,$9, $10, $11, $12)',[vid,
        pid, user_id, vehicle_code, license_no, engine_no, average_mileage, is_transfer, receipt_no,receipt_date, last_insurance_company,
        insurance_due_date], (err: Error) => {
          if (err) {
            log.error(err, 'query error');
          }else{
            let vehicle = {id:vid, owner:{id:pid, user_id:user_id, name:name, identity_no:identity_no,phone:phone}, drivers:[], vehicle_code:vehicle_code, license_no:license_no, 
            engine_no:engine_no, average_mileage:average_mileage, is_transfer:is_transfer, receipt_no:receipt_no, receipt_date:receipt_date, last_insurance_company:last_insurance_company, insurance_due_date:insurance_due_date};
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
});

processor.call('setDriverInfo', (db: PGClient, cache: RedisClient, done: DoneFunction, pid, did, vid, name,identity_no,phone,is_primary) => {
  log.info('setDriverInfo');
  //insert a record into person 
  db.query('INSERT INTO person (id,name,identity_no,phone) VALUES ($1, $2, $3, $4, $5)',[pid,
   name, identity_no, phone], (err: Error) => {
     if (err) {
      log.error(err, 'query error');
     }else{
        db.query('INSERT INTO drivers (id, vid, pid, is_primary) VALUES ($1, $2, $3, $4)',[did, vid, pid, is_primary], 
        (err: Error) => {
          if (err) {
            log.error(err, 'query error');
          }else{
            let multi = cache.multi();
            let vehicle=multi.hget("vehicle-entities",vid);
            vehicle["drivers"].push({id:pid, name:name, identity_no: identity_no ,phone:phone});
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

processor.call('changeDriverInfo', (db: PGClient, cache: RedisClient, done: DoneFunction, vid, pid, name, identity_no, phone) => {
  log.info('changeDriverInfo');
  db.query('UPDATE person SET name=$1, identity_no=$2, phone=$3 WHERE id=$4',[name, identity_no, phone, pid],(err: Error) => {
      if (err) {
        log.error(err);
      }else{
        let multi = cache.multi();
        let vehicle = multi.hget("vehicle-entities",vid);
        let drivers = vehicle["drivers"];
        let new_drivers = [];
        for(let driver of drivers){
          if(driver.id != pid){
            new_drivers.push(driver);
          }
        }
        new_drivers.push({id:pid, name:name,identity_no:identity_no, phone:phone});
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

processor.call('uploadDriverImages', (db: PGClient, cache: RedisClient, done: DoneFunction, vid, driving_frontal_view, driving_rear_view, identity_frontal_view, identity_rear_view, license_frontal_views) => {
  log.info('uploadDriverImages');
  db.query('UPDATE vehicles SET driving_frontal_view=$1, driving_rear_view=$2 WHERE id=$3',[driving_frontal_view, driving_rear_view, vid],(err: Error) => {
      if (err) {
        log.error(err);
      }else{
        db.query('UPDATE person SET identity_frontal_view=$1, identity_rear_view=$2 WHERE id in (SELECT owner FROM vehicles WHERE id = $3)',[identity_frontal_view, identity_rear_view, vid],(err: Error) => {
          if (err) {
            log.error(err);
          }else{
            let pids = [];
            let images = [];
            let obj = null;
            for(let key in license_frontal_views){
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
            let vehicle=multi.hget("vehicle-entities",vid);
            vehicle["driving_frontal_view"] = driving_frontal_view;
            vehicle["driving_rear_view"] = driving_rear_view;
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

processor.call('getVehicleModelsByMake', (db: PGClient, cache: RedisClient, done: DoneFunction,args) => {
  log.info('getVehicleModelsByMake');
  for(let arg of args){
    db.query('INSERT INTO vehicle_model(vehicle_code,vin_code,vehicle_name,brand_name,family_name,body_type,engine_number,engine_desc,gearbox_name,year_pattern,\
    group_name,cfg_level,purchase_price,purchase_price_tax,seat,effluent_standard,pl,fuel_jet_type,driven_type) VALUE($1, $2, $3, $4, $5, $6, $7, $8 ,$9, $10,\
    $11, $12, $13, $14, $15, $16, $17, $18, $19)', [arg.vehicleCode, arg.vin, arg.vehicleName,arg.brandName,arg.familyName,arg.bodyType,
    , arg.engineNumber, arg.engineDesc, arg.gearboxName, arg.yearPattern, arg.groupName, arg.cfgLevel, arg.purchasePrice, arg.purchasePriceTax, arg.seat,
    arg.effluentStandard, arg.pl, arg.fuelJetType, arg.drivenType], (err: Error) => {
      if (err) {
        log.error(err, 'query error');
        return;
      }
      let vehicle = arg;
      let multi = cache.multi();
      multi.hset("vehicle-model-entities", vehicle.vehicleCode, JSON.stringify(vehicle));
      multi.sadd("vehicle-model", vehicle.vehicleCode);
      multi.exec((err, replies) => {
        if (err) {
          log.error(err);
        }
        done(); // close db and cache connection
      });
  });
 }
});


log.info('Start processor at %s', config.addr);

processor.run();

