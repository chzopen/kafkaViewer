package per.chzopen.kafkaViewer.gui;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyAdapter;
import java.awt.event.KeyEvent;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;

import org.apache.curator.framework.CuratorFramework;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import per.chzopen.kafkaViewer.common.ConsoleContent;
import per.chzopen.kafkaViewer.common.Event;
import per.chzopen.kafkaViewer.common.ServiceStatus;
import per.chzopen.kafkaViewer.common.define.Releasable;
import per.chzopen.kafkaViewer.utils.ArrayUtils;

public class TopicTabPanel extends JPanel implements Releasable
{

	private static final long serialVersionUID = 1L;
	
	private static Logger logger = LoggerFactory.getLogger(TopicTabPanel.class);
	
	private TopicTabPanel _this = this;

	private CuratorFramework curatorClient;
	private String topic;
	private String clientId;
	private String groupId;
	
	
	private KafkaConsumer<String, String> consumer;
	private List<TopicPartition> partitions = new ArrayList<>();
	private ConsoleContent consoleContent = new ConsoleContent(10000);
	
	
	
	private JNumberField lastNInput;
	
	private JButton subscribeButton1;
	private JButton subscribeButton2;
	private JButton subscribeButton3;
	private JButton stopButton;
	
	private JTextArea textArea;
	
	private ConsumerRunnable consumerRunnable = new ConsumerRunnable();
	
	public TopicTabPanel(String topic)
	{
		if( topic==null )
		{
			throw new IllegalArgumentException("argument 'topic' is null");
		}
		this.topic = topic;
		initUI();
		initEvent();
	}
	
	
	private void initEvent()
	{
		// 从开始处订阅消息
		subscribeButton1.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent e)
			{
				switchButtonsLook(subscribeButton1);
				consumerRunnable.close();
				consumerRunnable.awaitTermination();
				fetchData();
				consumer.seekToBeginning(partitions);
				consumerRunnable.startConsumer();
			}
		});
		
		// 从结尾处订阅消息
		subscribeButton2.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent e)
			{
				switchButtonsLook(subscribeButton2);
				consumerRunnable.close();
				consumerRunnable.awaitTermination();
				fetchData();
				consumer.seekToEnd(partitions);
				consumerRunnable.startConsumer();
			}
		});
		
		// 订阅最后N条消息
		subscribeButton3.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent e)
			{
				switchButtonsLook(subscribeButton3);
				consumerRunnable.close();
				consumerRunnable.awaitTermination();
				fetchData();
				consumer.seekToBeginning(partitions);
				for( TopicPartition topicPartiton: partitions )
				{
					long beginning = consumer.position(topicPartiton);
					consumer.seekToEnd(Arrays.asList(topicPartiton));
					long ending = consumer.position(topicPartiton);
					if( ending - lastNInput.getLong() > beginning )
					{
						consumer.seek(topicPartiton, ending - lastNInput.getLong());
					}
					else
					{
						consumer.seekToBeginning(Arrays.asList(topicPartiton));
					}
				}
				consumerRunnable.startConsumer();
			}
		});
		
		//
		stopButton.addActionListener(new ActionListener()
		{
			public void actionPerformed(ActionEvent e)
			{
				switchButtonsLook(stopButton);
				consumerRunnable.close();
			}
		});
		
		//
		textArea.addKeyListener(new KeyAdapter()
		{
			public void keyTyped(KeyEvent e)
			{
				consoleContent.clear();
				consoleContent.addLine(textArea.getText());
			}
		});
	}
	
	private void switchButtonsLook(JButton focusButton)
	{
		toFocusName(subscribeButton1, subscribeButton1==focusButton);
		toFocusName(subscribeButton2, subscribeButton2==focusButton);
		toFocusName(subscribeButton3, subscribeButton3==focusButton);
		toFocusName(stopButton,       stopButton==focusButton);
	}
	
	private void toFocusName(JButton btn, boolean focus)
	{
		if( focus )
		{
			if( !btn.getText().startsWith("*") )
			{
				btn.setText("*"+btn.getText().trim());
			}
		}
		else
		{
			if( btn.getText().startsWith("*") )
			{
				btn.setText(" "+btn.getText().trim().substring(1));
			}
		}
	}
	
	public void setCuratorClient(CuratorFramework curatorClient)
	{
		this.curatorClient = curatorClient;
	}
	
	private void initUI()
	{
		this.setLayout(new GridBagLayout());

		GridBagConstraints gbc = null;
		int gridy = -1;
		
		// row 0
		{
			gridy++;

			gbc = new GridBagConstraints();
			gbc.gridx = 0;
			gbc.gridy = gridy;
			this.add(Box.createRigidArea(new Dimension(5, 5)), gbc);
		}
		
		// row 1
		{
			gridy++;

			gbc = new GridBagConstraints();
			gbc.gridx = 3;
			gbc.gridy = gridy;
			this.add(Box.createRigidArea(new Dimension(100, 5)), gbc);
			
			JLabel subscribeLabel = new JLabel("Subscribe from beginning:");
			gbc = new GridBagConstraints();
			gbc.gridx = 4;
			gbc.gridy = gridy;
			gbc.anchor = GridBagConstraints.WEST;
			this.add(subscribeLabel, gbc);

			subscribeButton1 = new JButton(" Subscribe!");
			gbc = new GridBagConstraints();
			gbc.gridx = 5;
			gbc.gridy = gridy;
			gbc.insets = new Insets(0, 5, 5, 10);
			this.add(subscribeButton1, gbc);
		}

		// row 2
		{
			gridy++;

			gbc = new GridBagConstraints();
			gbc.gridx = 3;
			gbc.gridy = gridy;
			gbc.weightx = 1;
			gbc.fill = GridBagConstraints.HORIZONTAL;
			this.add(Box.createRigidArea(new Dimension(100, 5)), gbc);
			
			JLabel subscribeLabel = new JLabel("Subscribe from ending:");
			gbc = new GridBagConstraints();
			gbc.gridx = 4;
			gbc.gridy = gridy;
			gbc.anchor = GridBagConstraints.WEST;
			this.add(subscribeLabel, gbc);

			subscribeButton2 = new JButton(" Subscribe!");
			gbc = new GridBagConstraints();
			gbc.gridx = 5;
			gbc.gridy = gridy;
			gbc.insets = new Insets(0, 5, 5, 10);
			this.add(subscribeButton2, gbc);
		}

		// row 3
		{
			gridy++;

			gbc = new GridBagConstraints();
			gbc.gridx = 3;
			gbc.gridy = gridy;
			this.add(Box.createRigidArea(new Dimension(100, 5)), gbc);
			
			JPanel subscribeLabelPanel = new JPanel();
			{
				lastNInput = new JNumberField(JNumberField.Type.UINT, "1");
				lastNInput.setDefaultValue("1");
				lastNInput.setPreferredSize(new Dimension(50, lastNInput.getSize().height));
				lastNInput.setMinimumSize(new Dimension(50, lastNInput.getSize().height));
						
				subscribeLabelPanel.setLayout(new BoxLayout(subscribeLabelPanel, BoxLayout.X_AXIS));
				subscribeLabelPanel.add(new JLabel("Subscribe last "));
				subscribeLabelPanel.add(lastNInput);
				subscribeLabelPanel.add(new JLabel(" Message(s):"));
			}
			gbc = new GridBagConstraints();
			gbc.gridx = 4;
			gbc.gridy = gridy;
			gbc.anchor = GridBagConstraints.WEST;
			this.add(subscribeLabelPanel, gbc);

			subscribeButton3 = new JButton(" Subscribe!");
			gbc = new GridBagConstraints();
			gbc.gridx = 5;
			gbc.gridy = gridy;
			gbc.insets = new Insets(0, 5, 5, 10);
			this.add(subscribeButton3, gbc);
		}

		// row 4
		{
			gridy++;

			stopButton = new JButton("*Stop!");
			gbc = new GridBagConstraints();
			gbc.gridx = 5;
			gbc.gridy = gridy;
			gbc.anchor = GridBagConstraints.EAST;
			gbc.insets = new Insets(0, 5, 5, 10);
			this.add(stopButton, gbc);
		}

		// row 5
		{
			gridy++;
			
			textArea = new JTextArea();
			
			gbc = new GridBagConstraints();
			gbc.gridx = 1;
			gbc.gridy = gridy;
			gbc.gridwidth = 10;
			gbc.weightx = 1;
			gbc.weighty = 1;
			gbc.fill = GridBagConstraints.BOTH;
			this.add(new JScrollPane(textArea), gbc);
		}
			
		// row -1
		{
			gridy++;

			gbc = new GridBagConstraints();
			gbc.gridx = 99;
			gbc.gridy = gridy;
			this.add(Box.createRigidArea(new Dimension(5, 5)), gbc);
		}
	
	}
	
	public void release() throws Exception
	{
		consumerRunnable.close();
		consumerRunnable.awaitTermination();
		if( consumer!=null )
		{
			consumer.close();
		}
	}
	
	public void initConsumer() throws Exception
	{
		List<String> brokerAddrs = new ArrayList<>();
		List<String> brokers = this.curatorClient.getChildren().forPath("/brokers/ids");
		for( String broker : brokers )
		{
			byte[] brokerData = this.curatorClient.getData().forPath("/brokers/ids/"+broker);
			JSONObject jsonObj = JSON.parseObject(new String(brokerData));
			brokerAddrs.add(jsonObj.getString("host")+":"+jsonObj.getString("port"));
		}
		
		//brokerAddrs.
		String brokersAddr = ArrayUtils.join(brokerAddrs).trim();
		if( brokersAddr.isEmpty() )
		{
			consoleContent.addLine("no borker exists.");
			textArea.setText(consoleContent.toString());
			return ;
		}
		
		_this.clientId = "client"+Math.abs(ThreadLocalRandom.current().nextLong());
		_this.groupId = "group"+Math.abs(ThreadLocalRandom.current().nextLong());
		
		Properties props = new Properties();
		props.put("bootstrap.servers",       brokersAddr);
		props.put("client.id",               _this.clientId);
		props.put("group.id",                _this.groupId);
		props.put("enable.auto.commit",      "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms",      "30000");
		props.put("max.poll.records",        "10240");
		props.put("key.deserializer",        "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer",      "org.apache.kafka.common.serialization.StringDeserializer");
		
		if( consumer!=null )
		{
			consumer.close();
		}
		
		consoleContent.clear();
		consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener()
		{
			public void onPartitionsRevoked(Collection<TopicPartition> partitions)
			{
				logger.info("partitionsRevoked: %s:[%s]", topic, partitions);
			}
			public void onPartitionsAssigned(Collection<TopicPartition> _partitions)
			{
				logger.info("partitionsAssigned: %s:[%s]", topic, _partitions);
				partitions.clear();
				partitions.addAll(_partitions);
			}
		});
		consumer.poll(0);
	}
	
	public void fetchData()
	{
		Map<Integer, ThisPartition> map = new TreeMap<Integer, ThisPartition>();
		
		// get beginning offset
		consumer.seekToBeginning(partitions);
		for( TopicPartition topicPartition : partitions )
		{
			ThisPartition thisPartition = map.get(topicPartition.partition());
			if( thisPartition==null )
			{
				thisPartition = new ThisPartition();
				map.put(topicPartition.partition(), thisPartition);
				thisPartition.partition = topicPartition.partition();
			}
			thisPartition.beginningOffset = consumer.position(topicPartition);
		}
		
		// get ending offset
		consumer.seekToEnd(partitions);
		for( TopicPartition topicPartition : partitions )
		{
			ThisPartition thisPartition = map.get(topicPartition.partition());
			thisPartition.endingOffset = consumer.position(topicPartition);
		}
		
		//
		consoleContent.addLine("");
		for( Entry<Integer,ThisPartition> entry: map.entrySet() )
		{
			ThisPartition thisPartition = entry.getValue();
			consoleContent.addLine(String.format("partition %s: beginningOffset=[%s], endingOffset=[%s], messageCount=[%s]", 
					thisPartition.partition, 
					thisPartition.beginningOffset, 
					thisPartition.endingOffset, 
					thisPartition.endingOffset - thisPartition.beginningOffset));
		}
		textArea.setText(consoleContent.toString());
	}
	
	
	
	
	/**
	 * 
	 */
	public class ConsumerRunnable implements Runnable
	{
		private AtomicReference<Thread> thread = new AtomicReference<Thread>();
		private AtomicReference<ServiceStatus> status = new AtomicReference<ServiceStatus>(ServiceStatus.idle);		// 
		private Event runningEvent = new Event(false);
		
		public void startConsumer()
		{
			if( thread.compareAndSet(null, new Thread(this)) )
			{
				status.set(ServiceStatus.starting);
				thread.get().start();
			}
			else
			{
				throw new IllegalStateException("consumer thread is running");
			}
		}
		
		public void close()
		{
			status.set(ServiceStatus.stopping);
		}
		
		public void run()
		{
			Thread.currentThread().setName("TopicTabPanel.ConsumerRunnable");
			status.set(ServiceStatus.running);
			runningEvent.set();
			while( status.get()==ServiceStatus.running )
			{
				ConsumerRecords<String, String> records = consumer.poll(100);
				if( records.count()==0 )
				{
					continue;
				}
				for (ConsumerRecord<String, String> record : records)
				{
					consoleContent.addLine(String.format("%s:%s, offset %s => %s: %s", record.topic(), record.partition(), record.offset(), record.key(), record.value()));
				}
				textArea.setText(consoleContent.toString());
				textArea.select(textArea.getText().length(), textArea.getText().length());
			}
			thread.set(null);
			status.set(ServiceStatus.stopped);
			runningEvent.clear();
		}
		
		public void awaitTermination()
		{
			runningEvent.waitIfTrue(0);
		}
	}
	
	/**
	 * 
	 */
	public static class ThisPartition
	{
		public int partition;
		public long beginningOffset;
		public long endingOffset;
	}
	
	/**
	 * 
	 */
	public static enum SubscriptionType
	{
		fromBeginning,
		fromEnding,
		lastesN
	}
}















