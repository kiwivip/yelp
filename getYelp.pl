#!/bin/env perl
# ==============================================================================
# function: crawl shops from 'www.yelp.com'
# author: kiwi
# date: 2012.7.5.test
# ==============================================================================
use 5.14.2;
use autodie;
no strict "refs";
use Fcntl qw(:flock SEEK_END);
use LWP::UserAgent;
use LWP::ConnCache;
use HTTP::Response;
use Parallel::ForkManager;
use Encode;
use Encode::Guess;
use Math::Random::Secure qw( rand irand );
use utf8;
binmode(STDIN, ':encoding(utf8)');
binmode(STDOUT, ':encoding(utf8)');
binmode(STDERR, ':encoding(utf8)');


# ------------------------------------------------------------------------------
my $file_urls = shift ;	# e.g. urls_shops_Desserts

my $dir_proxy = "/home/kiwi/Proxy";
my $file_bad_proxy = "bad_proxy_yelp";
# ------------------------------------------------------------------------------
die "Sorry !! I need the url_list you want to crawl !" unless defined $file_urls ;

#chomp($file_urls);
#my ($cat) = $file_urls =~ /_([^_]*?)$/ ;

my %proxys = %{getProxys()};
say "Available proxys : " . ~~ keys %proxys ;
# ------------------------------------------------------------------------------

my $max_process = 30;
my $pm = new Parallel::ForkManager( $max_process );
# ------------------------------------------------------------------------------
my $ua = LWP::UserAgent->new();
	
$ua -> agent("Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:12.0) Gecko/20100101 Firefox/12.0");
$ua -> timeout(10);
$ua -> {proxy} = {};
$ua -> cookie_jar({});
$ua -> default_header(
	"Accept" => 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8' ,
	"Accept-Language" => 'zh-cn,zh;q=0.8,en-us;q=0.5,en;q=0.3'
);
$ua -> requests_redirectable;

my $conncache = new LWP::ConnCache;
$ua -> conn_cache($conncache);	 
# ------------------------------------------------------------------------------
open my $fh_urlList , "<" , $file_urls;
while(<$fh_urlList>)
{
	
        $pm -> start and next;
        coreget($_);
        $pm -> finish;
}
$pm -> wait_all_children;


# ============================== functions =====================================

sub lock_file
{
	my ($FH) = @_;
	flock($FH, LOCK_EX) or die "Cannot lock file - $!\n";
	#print "file locked!\n";
	# and, in case someone appended while we were waiting...
	seek($FH, SEEK_END,0) or die "Cannot seek - $!\n";
}

sub unlock_file
{
	my ($FH) = @_;
	flock($FH, LOCK_UN) or die "Cannot unlock file - $!\n";
        #print "file unlocked!\n";
}


sub coreget
{
        my $info = decode2utf8($_[0]);
        chomp($info);
        my ($url,$city,$cat) = split "\t" ,$info ;
	
        $url  =~ s/^\s+|\s+$//g;
        $city =~ s/^\s+|\s+$//g;
	$cat  =~ s/^\s+|\s+$//g;
        
        my ($shopName) = $url =~ /biz\/(.*?)$/i ;
	my $id = $city."_".$shopName;
        
        my $dir = "./YelpShops/$cat/" ;
	my $file = $dir.$id ;
	
	my (@temp,$page,$log) ;
	# if file exist or not range given , then quit
	#{
		return 0 if -e $file ;
		
		@temp = @{crawl($url,1)};	
		$page = $temp[0];
		
		if ( ! pageCheck($page) )
		{
			$log = $url."\t".$page."\t".$temp[1]."\n";
			print decode2utf8($log);
		
			
			# if Can't connect to the proxy , log it into file_bad_proxy
			if($page =~ /^403|^50/)
			{
				open my $fh_bad_proxy , ">>" , $file_bad_proxy;
				print $fh_bad_proxy $temp[1]."\n";
				
			}
			delete $proxys{$temp[1]};
			
			return 0 ;
		}
		#redo unless -e $file;
	#}

	myDir($dir);
       
	open my $fh_text , ">:utf8" , $file ;
        lock_file($fh_text);
	print $fh_text $page;
	unlock_file($fh_text);
        
	
	say "$file\t OK ❤ \t$temp[1]";
}

sub getProxys
{
	my $ref_proxys ;
	
	# read all proxys files to get a proxys list
	open my $fh_proxys , "cat $dir_proxy/* |";
	binmode($fh_proxys, ':encoding(utf8)');
	my %proxy;
	foreach(<$fh_proxys>)
	{
		chomp;
		next if /^$/;
		my ($proxy) = $_ =~ /^([\d.:]+)@/ ;
		$proxy{$proxy} = 1 ;
	}

	# read the bad proxy file to take the useless proxy out from the list
	open my $fh_bad , "<:utf8" ,$file_bad_proxy ;
	foreach(<$fh_bad>)
	{
		chomp;
		next if /^$/;
		if(exists $proxy{$_}){
			delete $proxy{$_};
		}
	}

	$ref_proxys = \%proxy;
	return $ref_proxys; 
}


sub myDir
{
        my $dir = shift;
        my $mod = shift || '777';
        if ( ! -e $dir )
        {
                system "mkdir -p -m $mod $dir";
        }
}

sub decode2utf8 
{
        my $data = shift || return;
        return $data if Encode::is_utf8( $data );
        #print "[data]" , $data , "\n";
        #print "[is]",Encode::is_utf8( $data ) , "\n";
        my $decoder = guess_encoding( $data );
        if( ! ref $decoder )
	{
		#$decoder = guess_encoding( $data , qw/euc-cn/ );
                if( ! ref $decoder )
		{
			#print "[decoder]" ,$decoder,"\n";
                        return $decoder;
                }
        }
        my $utf8 = $decoder->decode($data);
        #print Encode::is_utf8( $utf8 ) , "\n";
        return $utf8;
}

sub pageCheck
{
	my $content = $_[0] ;
	if ($content ~~ /yelp\.init\.bizDetails\.page/ and $content ~~ /<\/html>$/){
		return 1 ;
	}
	return 0 ;
}

sub crawl
{
	my $url = $_[0];
	my $flag = $_[1];
  
	my $times = 0;
	my ($content,$response,$proxy);
	
	while ($times < 50)
	{
		if($flag == 1)
		{
			my $count = keys %proxys;
			my @temp = keys %proxys;
			$proxy = $temp[rand $count];
			$ua -> proxy("http","http://".$proxy);
		}
		
		$response = $ua -> get($url);
		if($response -> is_success)
		{
			$content = $response -> content;
			last if pageCheck($content) ;
		}
		#select(undef, undef, undef, 0.25);	# if use proxy,don't need to sleep
		$times++;
	}
	
	if ( $times == 50 ){
		#print "get failed!\n";
		return [$response -> status_line,$proxy];
	}

	
	return [$content,$proxy];
}
