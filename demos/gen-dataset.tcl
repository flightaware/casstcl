

set firstNames [list "安 An" "波 Bo" "成 Cheng" "德 De" "东 Dong" "峰 Feng" "刚 Gang" "国 Guo" "辉 Hui" "健 Jian" "杰 Jie" "康 Kang" "亮 Liang" "宁 Ning" "鹏 Peng" "涛 Tao" "伟 Wei" "勇 Yong" "文 Wen" "爱 Ai" "碧 Bi" "彩 Cai" "丹 Dan" "芳 Fang" "红 Hong" "惠 Hui" "娟 Juan" "兰 Lan" "莉 Li" "丽 Li" "莲 Lian" "娜 Na" "妮 Ni" "倩 Qian" "琼 Qiong" "珊 Shan" "淑 Shu" "婷 Ting" "霞 Xia" "娴 Xian" "嫣 Yan" "云 Yun" "贞 Zhen" Noel Aron Alesandro Enrik Flavio Luis Aleskio Oliver Jack Harry Jacob Charlie Thomas Oscar William James "Alexander (Александр)" "Sergei (Сергей)" "Dmitry (Дмитрий)" "Andrei (Андрей)" "Alexey (Алексей)" "Maxim (Максим)" "Evgeny (Евгений)" "Ivan (Иван)" "Mikhail (Михаил)" "Ilya (Илья)" Laia Carlota Emma Lara Martina Aina Maria Blanca Laura "Anastasia (Анастасия)" "Yelena (Елена)" "Olga (Ольга)" "Natalia (Наталья)" "Yekaterina (Екатерина)" "Anna (Анна)" "Tatiana (Татьяна)" "Maria (Мария)" "Irina (Ирина)" "Yulia (Юлия)" Amelia Olivia Emily Jessica Ava Isla Poppy Isabella Sophie Emilia Ella Helmi Linnea Birta Elizabete Lachlan Charlotte Ruby Chloe Mia Maia "Maryam مريم" "Jana جنى" "Noor نور" Thiago Juan Felipe Daniel Mark Karl Nugget Sean Adam Jeff James Ashley Sherron Lindsay Ryan Tim Vakesa Vishan Zachery Kim Philip Rijul Baron Bill Debbie Aarav Mohammad Vihaan Aditya Sai Arjun Aryan Reyansh Vivaan Krishna Aadhya Ananya Saanvi Aaradhya Anaya Aanya Anika Pari Myra Shanaya Ellyn Karyn Annette Agnes Joshua Nathan Dorothy Kristopher Shawn Morgan Robert Eugene]

set lastNames [list Devi Singh Kumar Das Kaur Cohen Levi Mizrachi Peretz ბერიძე მამედოვი 王 李 张 刘 陈 杨 黄 Baker Duell McNett Zhōu Sulak "佐藤" "鈴木" "高橋" "田中" "渡辺" "伊藤" 金김李Santos Reyes Cruz Bautista Ocampo "del Rosario" Gonzales Lopez Garcia "dela Cruz" Mendoza Yılmaz Kaya Demir Şahin Çelik Yıldız "Nguyễn 阮" "Trần 陳" "Lê 黎" "Phạm 范" Díaz Mora Gruber Huber Bauer Wagner Məmmədov Əliyev Ivanoŭ Peeters Janssens Maes Jacobs Mertens Horvat Nováková Svobodová Dvořáková Nielsen Jensen Sørensen Jørgensen Papadopoulos Angelopoulos Nagy Tóth Balogh Farkas Murphy "Ó Ceallaigh" Rossi Russo Ferrari Esposito Bianchi Romano Ricci "De Luca" Giordano Rizzo Lombardi Moretti Sante Ravelli Zogaj Gashi Grasniqi Morina Kelmendi Beqiri Jakupi Kastrati Avdiu Borg Camilleri Vella Farrugia Andov Angelov Kitanovski "De Jong" "De Vries" "Van Dijk" Bakker Nowak Kowalski Wiśniewski Wójcik Смирно́в Ивано́в Кузнецо́в Козло́в Лебедев Соловьёв Виногра́дов Jovanović Zupančič Smith Johnson Williams Brown Jones Miller Davis Garcia Rodriguez Wilson Martinez Anderson Taylor Thomas Hernandez Moore Martin Jackson]

proc gen_nameset {} {
	global firstNames lastNames

	foreach last2 $lastNames {
		foreach first $firstNames {
			foreach last1 $lastNames {
				puts [list $first [list $last1 $last2]]
			}
		}
	}
}

gen_nameset
