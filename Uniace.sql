-- Viết function phát hiện data có phải dạng timestamp hay không nếu đúng trả về 1 sai trả về -1
create or replace function checkdatatype (n varchar)
returns numeric 
as $$
declare  
thoi_gian_sx timestamp ;
begin  	 
	 thoi_gian_sx := n:: timestamp  ;
	 return 1 ;
	 exception 
	 when others then 
	 return -1;
	
end;	
$$ language plpgsql;

create or replace function checkdatatype1 (n varchar)
returns numeric 
as $$
declare  
thoi_gian_sx timestamp  ;
begin  	 
	 thoi_gian_sx :=  TO_TIMESTAMP(n, 'DD-MM-YYYY HH24:MI:SS') ;
	 return 1 ;
	 exception 
	 when others then 
	 return -1;
	
end;	
$$ language plpgsql;
-- Lọc những dòng mà cột date bị lỗi 

select*, 
				case 
				when message_id is null then null 
				when checkdatatype(message_id)=1 then message_id 
				when checkdatatype1(message_id)= 1 then  TO_TIMESTAMP(message_id, 'DD-MM-YYYY HH24:MI:SS'):: text
				else 'Fix'
				end as date_fix_1

select email, type, name, title, ma_url, ma_referrer, ma_path, ip_address,cuid,date_fix,
message_id, template_id, list_id, form_id, campaign_id, campaign_name, scenario_id, url, link, tag
from (
		select*, 
				case 
				when date is null  then null 
				when checkdatatype (date)=1 then date
				when checkdatatype1 (date)= 1 then  TO_TIMESTAMP(date, 'DD-MM-YYYY HH24:MI:SS'):: text
		else 'Fix'
				end as date_fix
		from (-- Gộp 3 bảng dữ liệu ban đầu 
				select * from uniace_1 u 
				union all
				select * from uniace_2 u2 
				union all 
				select * from uniace_3 u3 
			 ) a
			 
	  ) b
where  date_fix <>'Fix';
create view uniace_fix_1 as 
select *, 
case 
				when message_id is null  then null 
				when checkdatatype (message_id)=1 then message_id 
				when checkdatatype1 (message_id)= 1 then  TO_TIMESTAMP(message_id , 'DD-MM-YYYY HH24:MI:SS'):: text
		        else 'Fix'
				end as date_fix_1
from (
      -- Check dữ liệu truyền vào ở cột date có phải dạng date time không , có phải ở dạng date time khác không, nếu lệch thì nó ở cột nào
		select*, 
				case 
				when date is null then null 
				when checkdatatype (date)=1 then date
				when checkdatatype1 (date)= 1 then  TO_TIMESTAMP(date, 'DD-MM-YYYY HH24:MI:SS'):: text
		        else 'Fix'
				end as date_fix
		from (
				select * from uniace_1 u 
				union all
				select * from uniace_2 u2 
				union all 
				select * from uniace_3 u3 
			 ) a
			 
	  ) b
where  date_fix ='Fix';
drop view if exists uniace_fix_2 ;
create view uniace_fix_2 as 
select *,
                case 
				when template_id  is null  then null 
				when checkdatatype (template_id)=1 then date
				when checkdatatype1 (template_id)= 1 then  TO_TIMESTAMP(template_id , 'DD-MM-YYYY HH24:MI:SS'):: text
		        else 'Fix'
				end as date_fix_2
from uniace_fix_1
where date_fix_1 ='Fix'
select * from uniace_fix_2 where date_fix_2 ='Fix'
-- Tạo bảng mới sau khi fix xong data bao gồm các cột, id, date, user_id, event_type, object_name, email, ma_referrer,ma_path 
drop table  if exists uniace_fix ;
create table uniace_fix as 
select ID,
case 
when created_time >'2021-08-31' or created_time< '2021-08-01' then TO_TIMESTAMP(cast(created_time as varchar ), 'YYYY-DD-MM HH24:MI:SS' )
else created_time 
end as created_time,
user_id,event_type, object_name ,email,ma_referrer ,ma_path 
from (
	select ip_address as ID, 
	case 
	when checkdatatype(date) =1 then date :: timestamp   
	else TO_TIMESTAMP(date, 'DD-MM-YYYY HH24:MI:SS') 
	end as created_time, cuid as user_id, type as event_type , name as object_name,email, ma_referrer,ma_path  
	from (
	-- Dữ liệu truyền vao ở cột date bị nhầm sang cột template_id 
	select email, type, name, title, ma_url, ip_address as ma_referrer,cuid as ma_path, date as ip_address ,
	message_id as cuid, template_id as date ,list_id as message_id ,form_id as template_id, list_id , form_id, campaign_id, campaign_name, scenario_id, url, link, tag
	from uniace_fix_2
	union all 
	-- Dữ liệu truyền ở cột date bị nhập nhầm sang cột message_id 
	select email, type, name, title, ma_url, ma_referrer, ip_address as ma_path ,cuid as ip_address ,date as cuid,
	message_id as date , template_id as message_id ,template_id, list_id , form_id, campaign_id, campaign_name, scenario_id, url, link, tag
	from uniace_fix_1
	where date_fix_1 <>'Fix'
	-- Dữ liệu truyền vào ở cột date là null và có dạng date time 
	union all 
	select email, type, name, title, ma_url, ma_referrer, ma_path, ip_address,cuid,date_fix,
	message_id, template_id, list_id, form_id, campaign_id, campaign_name, scenario_id, url, link, tag
	from (
			select*, 
					case 
					when date is null then null 
					when checkdatatype (date)=1 then date
					when checkdatatype1 (date)= 1 then  TO_TIMESTAMP(date, 'DD-MM-YYYY HH24:MI:SS'):: text
			else 'Fix'
					end as date_fix
			from (
					select * from uniace_1 u 
					union all
					select * from uniace_2 u2 
					union all 
					select * from uniace_3 u3 
				 ) a
				 
		  ) b
	where  date_fix <>'Fix'
	) c 
	)x

